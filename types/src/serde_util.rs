//! Serde helpers shared by the manifest and the host envelopes.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};

pub(crate) fn yes() -> bool {
    true
}

pub(crate) fn is_yes(value: &bool) -> bool {
    *value
}

/// A key that is present, `null` included: absence is the field's default.
pub(crate) fn present<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

/// A map's entries in the order of their keys, so what is hashed of it is
/// the same in every process.
pub(crate) fn sorted<S: Serializer>(
    map: &HashMap<String, String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    map.iter().collect::<BTreeMap<_, _>>().serialize(serializer)
}

/// Bytes as standard base64 text.
pub(crate) mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        STANDARD
            .decode(String::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }

    pub(crate) mod option {
        use super::STANDARD;
        use base64::Engine;
        use serde::{Deserialize, Deserializer, Serializer};

        pub(crate) fn serialize<S: Serializer>(
            value: &Option<Vec<u8>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match value {
                Some(bytes) => serializer.serialize_some(&STANDARD.encode(bytes)),
                None => serializer.serialize_none(),
            }
        }

        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Vec<u8>>, D::Error> {
            Option::<String>::deserialize(deserializer)?
                .map(|text| STANDARD.decode(text).map_err(serde::de::Error::custom))
                .transpose()
        }
    }
}
