//! The envelopes of the host functions and the guest request, shared by
//! the guest SDK and the platform so the two sides cannot drift.

pub mod ext;
pub mod http;
pub mod nats;
pub mod request;
pub mod storage;

#[cfg(test)]
pub(crate) mod tests {
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use serde_json::Value;
    use std::fmt::Debug;

    pub(crate) fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(
        value: &T,
        wire: Value,
    ) {
        assert_eq!(serde_json::to_value(value).expect("serialize"), wire);
        assert_eq!(
            &serde_json::from_value::<T>(wire).expect("deserialize"),
            value
        );
    }

    pub(crate) fn refused<T: DeserializeOwned + Debug>(wire: Value) {
        let parsed = serde_json::from_value::<T>(wire.clone());
        assert!(parsed.is_err(), "{wire} parsed as {parsed:?}");
    }
}
