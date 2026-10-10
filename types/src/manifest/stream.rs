use serde::{Deserialize, Serialize};

/// A JetStream stream the service needs. The platform creates it, or
/// extends the one that exists; a deploy never deletes a stream or removes
/// a subject from one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    pub subjects: Vec<String>,
    pub retention: StreamRetention,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_msgs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "StreamStorage::is_file")]
    pub storage: StreamStorage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_window_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "DiscardSpec::is_old")]
    pub discard: DiscardSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamRetention {
    #[default]
    Limits,
    WorkQueue,
    Interest,
}

impl StreamRetention {
    pub fn as_str(self) -> &'static str {
        match self {
            StreamRetention::Limits => "limits",
            StreamRetention::WorkQueue => "work_queue",
            StreamRetention::Interest => "interest",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStorage {
    #[default]
    File,
    Memory,
}

impl StreamStorage {
    pub fn is_file(&self) -> bool {
        *self == StreamStorage::File
    }
}

/// What a full stream does with a new message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscardSpec {
    /// Drops its oldest message.
    #[default]
    Old,
    /// Refuses the new one.
    New,
}

impl DiscardSpec {
    pub fn is_old(&self) -> bool {
        *self == DiscardSpec::Old
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_minimal_stream_carries_only_what_it_declares() {
        let stream = StreamDefinition {
            name: "NOTES".into(),
            subjects: vec!["notes.created".into()],
            retention: StreamRetention::Limits,
            max_age_secs: None,
            max_msgs: None,
            max_bytes: None,
            storage: StreamStorage::File,
            duplicate_window_secs: None,
            discard: DiscardSpec::Old,
            description: None,
        };
        let wire = json!({"name": "NOTES", "subjects": ["notes.created"], "retention": "limits"});
        assert_eq!(serde_json::to_value(&stream).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<StreamDefinition>(wire).unwrap(),
            stream
        );
    }

    #[test]
    fn every_field_round_trips() {
        let stream = StreamDefinition {
            name: "RECEIPTS".into(),
            subjects: vec!["receipts.>".into()],
            retention: StreamRetention::WorkQueue,
            max_age_secs: Some(2_592_000),
            max_msgs: Some(100_000),
            max_bytes: Some(268_435_456),
            storage: StreamStorage::Memory,
            duplicate_window_secs: Some(120),
            discard: DiscardSpec::New,
            description: Some("quote jobs".into()),
        };
        let wire = json!({
            "name": "RECEIPTS",
            "subjects": ["receipts.>"],
            "retention": "work_queue",
            "max_age_secs": 2592000,
            "max_msgs": 100000,
            "max_bytes": 268435456,
            "storage": "memory",
            "duplicate_window_secs": 120,
            "discard": "new",
            "description": "quote jobs"
        });
        assert_eq!(serde_json::to_value(&stream).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<StreamDefinition>(wire).unwrap(),
            stream
        );
        assert_eq!(
            serde_json::to_value(StreamRetention::Interest).unwrap(),
            json!(StreamRetention::Interest.as_str())
        );
    }
}
