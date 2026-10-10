//! Where the SDK's functions go in a native build: to the capabilities
//! installed for the current thread (`test-host`), and to what they always
//! did outside the platform when there is none. Every capability speaks the
//! envelopes of the wasm ABI, so a test runs the SDK's own encoding.

pub mod cache;
pub mod clock;
pub mod config;
pub mod db;
pub mod ext;
pub mod http;
pub mod image;
pub mod jobs;
pub mod nats;
pub mod service_call;
pub mod storage;

use std::rc::Rc;

/// The capabilities of one thread. A capability left `None` leaves its
/// functions as they are without a host.
#[derive(Clone, Default)]
pub struct HostSet {
    pub db: Option<Rc<dyn db::DbHost>>,
    pub http: Option<Rc<dyn http::HttpHost>>,
    pub jobs: Option<Rc<dyn jobs::JobsHost>>,
    pub nats: Option<Rc<dyn nats::NatsHost>>,
    pub storage: Option<Rc<dyn storage::StorageHost>>,
    pub cache: Option<Rc<dyn cache::CacheHost>>,
    pub config: Option<Rc<dyn config::ConfigHost>>,
    pub clock: Option<Rc<dyn clock::ClockHost>>,
    pub image: Option<Rc<dyn image::ImageHost>>,
    #[cfg_attr(not(feature = "service-call"), allow(dead_code))]
    pub service_call: Option<Rc<dyn service_call::ServiceCallHost>>,
    #[cfg_attr(not(feature = "ext-call"), allow(dead_code))]
    pub ext: Option<Rc<dyn ext::ExtHost>>,
    /// What `context::tenant()` answers.
    pub tenant: String,
}

#[cfg(feature = "test-host")]
thread_local! {
    static CURRENT: std::cell::RefCell<Option<Rc<HostSet>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-host")]
pub fn current() -> Option<Rc<HostSet>> {
    CURRENT.with(|current| current.borrow().clone())
}

#[cfg(not(feature = "test-host"))]
pub(crate) const fn current() -> Option<Rc<HostSet>> {
    None
}

/// Run `body` with `hosts` installed on this thread; what was installed
/// before is back when it returns or unwinds.
#[cfg(feature = "test-host")]
pub fn with<T>(hosts: HostSet, body: impl FnOnce() -> T) -> T {
    struct Restore(Option<Rc<HostSet>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|current| *current.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(CURRENT.with(|current| current.replace(Some(Rc::new(hosts)))));
    body()
}

pub(crate) fn db() -> Option<Rc<dyn db::DbHost>> {
    current().and_then(|hosts| hosts.db.clone())
}

pub(crate) fn http() -> Option<Rc<dyn http::HttpHost>> {
    current().and_then(|hosts| hosts.http.clone())
}

pub(crate) fn cache() -> Option<Rc<dyn cache::CacheHost>> {
    current().and_then(|hosts| hosts.cache.clone())
}

pub(crate) fn config() -> Option<Rc<dyn config::ConfigHost>> {
    current().and_then(|hosts| hosts.config.clone())
}

pub(crate) fn image() -> Option<Rc<dyn image::ImageHost>> {
    current().and_then(|hosts| hosts.image.clone())
}

#[cfg(feature = "service-call")]
pub(crate) fn service_call() -> Option<Rc<dyn service_call::ServiceCallHost>> {
    current().and_then(|hosts| hosts.service_call.clone())
}

#[cfg(feature = "ext-call")]
pub(crate) fn ext() -> Option<Rc<dyn ext::ExtHost>> {
    current().and_then(|hosts| hosts.ext.clone())
}

pub(crate) fn tenant() -> Option<String> {
    current().map(|hosts| hosts.tenant.clone())
}

pub(crate) fn jobs() -> Option<Rc<dyn jobs::JobsHost>> {
    current().and_then(|hosts| hosts.jobs.clone())
}

pub(crate) fn nats() -> Option<Rc<dyn nats::NatsHost>> {
    current().and_then(|hosts| hosts.nats.clone())
}

pub(crate) fn clock() -> Option<Rc<dyn clock::ClockHost>> {
    current().and_then(|hosts| hosts.clock.clone())
}

pub(crate) fn storage() -> Option<Rc<dyn storage::StorageHost>> {
    current().and_then(|hosts| hosts.storage.clone())
}

#[cfg(all(test, feature = "test-host"))]
mod tests {
    use super::*;
    use crate::testing::MockHttp;

    fn installed() -> Option<Rc<dyn http::HttpHost>> {
        http()
    }

    #[test]
    fn hosts_are_installed_for_the_closure_and_restored_after() {
        assert!(installed().is_none());
        let outer = HostSet {
            http: Some(Rc::new(MockHttp::new())),
            ..Default::default()
        };
        with(outer, || {
            assert!(installed().is_some());
            with(HostSet::default(), || assert!(installed().is_none()));
            assert!(installed().is_some());
        });
        assert!(installed().is_none());
    }

    #[test]
    fn a_panic_leaves_nothing_installed() {
        let unwound = std::panic::catch_unwind(|| {
            let hosts = HostSet {
                http: Some(Rc::new(MockHttp::new())),
                ..Default::default()
            };
            with(hosts, || panic!("handler"))
        });
        assert!(unwound.is_err());
        assert!(installed().is_none());
    }
}
