//! The `__delivery` block of a v2 guest response: how the platform should
//! settle the message or job the guest just handled.

use serde::{Deserialize, Serialize};

/// The key of the block in a v2 response envelope.
pub const DELIVERY_KEY: &str = "__delivery";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum DeliveryControl {
    /// Deliver again after this long.
    RetryAfter { retry_after_ms: u64 },
    /// Never deliver again; `terminal` says why.
    Terminal { terminal: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_wire_shapes_are_pinned() {
        for (control, wire) in [
            (
                DeliveryControl::RetryAfter {
                    retry_after_ms: 1500,
                },
                json!({"retry_after_ms": 1500}),
            ),
            (
                DeliveryControl::Terminal {
                    terminal: "unknown book".into(),
                },
                json!({"terminal": "unknown book"}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&control).expect("serialize"), wire);
            assert_eq!(
                serde_json::from_value::<DeliveryControl>(wire).expect("deserialize"),
                control
            );
        }
    }

    #[test]
    fn anything_else_is_refused() {
        for wire in [
            json!({}),
            json!({"retry_after_ms": -1}),
            json!({"retry_after_ms": "soon"}),
            json!({"terminal": 7}),
            json!({"retry_after_ms": 1, "terminal": "both"}),
            json!({"retry_after_ms": 1, "extra": true}),
            json!("terminal"),
        ] {
            assert!(
                serde_json::from_value::<DeliveryControl>(wire.clone()).is_err(),
                "{wire}"
            );
        }
    }
}
