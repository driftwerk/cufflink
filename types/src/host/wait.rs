//! How a host call waits: holding the guest's execution slots, or giving
//! them back until its answer arrives.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wait {
    #[default]
    Hold,
    Release,
}

impl Wait {
    pub fn is_hold(&self) -> bool {
        matches!(self, Wait::Hold)
    }
}

#[cfg(test)]
mod tests {
    use super::Wait;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    #[test]
    fn a_wait_is_hold_unless_it_says_release() {
        round_trip(&Wait::Hold, json!("hold"));
        round_trip(&Wait::Release, json!("release"));
        refused::<Wait>(json!("park"));
        assert!(Wait::default().is_hold());
        assert!(!Wait::Release.is_hold());
    }
}
