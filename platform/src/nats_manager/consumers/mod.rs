pub mod apply;
pub mod desired;
pub mod naming;
pub mod plan;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LoopKey {
    pub tenant_slug: String,
    pub service_name: String,
    pub handler: String,
    pub subject: String,
}
