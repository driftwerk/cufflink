//! Service configuration a test sets.

use crate::host::config::ConfigHost;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

/// The values `config::get` answers. Clones share one map.
#[derive(Clone, Default)]
pub struct ConfigMap {
    values: Rc<RefCell<BTreeMap<String, String>>>,
}

impl ConfigMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: &str, value: &str) {
        self.values
            .borrow_mut()
            .insert(key.to_string(), value.to_string());
    }

    pub fn remove(&self, key: &str) {
        self.values.borrow_mut().remove(key);
    }
}

impl ConfigHost for ConfigMap {
    fn get(&self, key: &str) -> Option<String> {
        self.values.borrow().get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::host::{self, HostSet};

    #[test]
    fn a_handler_reads_what_the_test_set() {
        let values = ConfigMap::new();
        values.set("PDF_RENDERER_URL", "https://pdf.example.com");
        let hosts = HostSet {
            config: Some(Rc::new(values.clone())),
            ..Default::default()
        };
        host::with(hosts, || {
            assert_eq!(
                config::get("PDF_RENDERER_URL").as_deref(),
                Some("https://pdf.example.com")
            );
            assert_eq!(config::get("MAIL_API_TOKEN"), None);
            values.set("MAIL_API_TOKEN", "t");
            assert_eq!(config::get("MAIL_API_TOKEN").as_deref(), Some("t"));
            values.remove("PDF_RENDERER_URL");
            assert_eq!(config::get("PDF_RENDERER_URL"), None);
        });
        assert_eq!(config::get("MAIL_API_TOKEN"), None);
    }
}
