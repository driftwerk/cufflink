use cufflink_types::ext::ExtFailure;
use std::fmt;

/// Why an extension failed a call: a `code` its caller can tell apart and
/// a message.
///
/// Any error converts into one with the code [`ExtError::FAILED`], so `?`
/// passes on the errors an extension meets; [`ExtError::new`] names another
/// code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtError {
    pub code: String,
    pub message: String,
}

/// What an extension answers.
pub type Result<T> = std::result::Result<T, ExtError>;

impl ExtError {
    /// The code of an error converted from any other.
    pub const FAILED: &'static str = "failed";

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl<E: Into<Box<dyn std::error::Error + Send + Sync>>> From<E> for ExtError {
    fn from(error: E) -> Self {
        Self::new(Self::FAILED, error.into().to_string())
    }
}

impl fmt::Display for ExtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl From<ExtError> for ExtFailure {
    fn from(error: ExtError) -> Self {
        Self {
            code: error.code,
            message: error.message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn converted<E: Into<Box<dyn std::error::Error + Send + Sync>>>(error: E) -> ExtError {
        ExtError::from(error)
    }

    #[test]
    fn any_error_converts_as_failed() {
        let parsed = "x".parse::<u32>().map_err(ExtError::from).unwrap_err();
        assert_eq!(
            parsed,
            ExtError::new("failed", "invalid digit found in string")
        );
        assert_eq!(
            converted("scope is required"),
            ExtError::new("failed", "scope is required")
        );
        assert_eq!(
            converted(String::from("no quota")),
            ExtError::new("failed", "no quota")
        );
        assert_eq!(
            converted(std::io::Error::other("disk gone")),
            ExtError::new("failed", "disk gone")
        );
    }

    #[test]
    fn question_mark_converts() {
        fn scope(payload: &serde_json::Value) -> Result<String> {
            let scope = payload["scope"].as_str().ok_or("scope is required")?;
            let length: u8 = scope.len().try_into()?;
            Ok(format!("{scope}:{length}"))
        }
        assert_eq!(
            scope(&serde_json::json!({})),
            Err(ExtError::new("failed", "scope is required"))
        );
        assert_eq!(
            scope(&serde_json::json!({"scope": "a".repeat(300)})).map_err(|e| e.code),
            Err("failed".into())
        );
        assert_eq!(
            scope(&serde_json::json!({"scope": "drive"})),
            Ok("drive:5".into())
        );
    }

    #[test]
    fn an_error_reads_and_travels_as_its_code_and_message() {
        let error = ExtError::new("quota", "daily quota exhausted");
        assert_eq!(error.to_string(), "quota: daily quota exhausted");
        assert_eq!(
            ExtFailure::from(error),
            ExtFailure {
                code: "quota".into(),
                message: "daily quota exhausted".into()
            }
        );
    }
}
