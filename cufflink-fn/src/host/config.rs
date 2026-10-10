//! Service configuration.

/// What answers `config::get`.
pub trait ConfigHost {
    fn get(&self, key: &str) -> Option<String>;
}
