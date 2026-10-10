use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use eyre::{Result, WrapErr};
use futures_util::stream::BoxStream;
use std::sync::Arc;

mod common;
mod gcs;
mod gcs_v4;
mod prefixed;
mod s3;

use gcs::GcsBackend;
use prefixed::PrefixedBackend;
use s3::S3Backend;
use std::path::Path;

const ENV_BACKEND: &str = "STORAGE_BACKEND";
const ENV_BUCKET: &str = "S3_BUCKET";
const ENV_LOCAL_DIR: &str = "STORAGE_LOCAL_DIR";
const DEFAULT_BUCKET: &str = "cufflink";

#[async_trait]
pub trait ObjectStorage: Send + Sync {
    async fn upload(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<()>;

    async fn upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<()>;

    async fn upload_to_bucket_with(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        options: &UploadOptions,
    ) -> Result<Uploaded>;

    async fn download(&self, key: &str) -> Result<Vec<u8>>;

    async fn download_from_bucket(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;

    async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>>;

    async fn head(&self, key: &str) -> Result<u64>;

    async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<()>;

    async fn delete(&self, key: &str) -> Result<()>;

    async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()>;

    async fn delete_prefix(&self, prefix: &str) -> Result<usize>;

    async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String>;

    async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String>;

    async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String>;

    async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String>;

    fn primary_bucket(&self) -> &str;

    fn backend_kind(&self) -> &'static str;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadOptions {
    pub content_type: String,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
}

impl UploadOptions {
    pub fn content_type(content_type: &str) -> Self {
        Self {
            content_type: content_type.to_string(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uploaded {
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: DateTime<Utc>,
}

pub struct ObjectReader {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub stream: BoxStream<'static, Result<Bytes>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    S3,
    Gcs,
    Memory,
    Local,
}

impl Backend {
    pub fn from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<Self> {
        Self::parse(vars(ENV_BACKEND).as_deref())
    }

    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw {
            Some("s3") | None => Ok(Self::S3),
            Some("gcs") => Ok(Self::Gcs),
            Some("memory") => Ok(Self::Memory),
            Some("local") => Ok(Self::Local),
            Some(other) => Err(unknown_backend(other)),
        }
    }

    pub fn is_ephemeral(self) -> bool {
        matches!(self, Self::Memory | Self::Local)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Memory => "memory",
            Self::Local => "local",
        }
    }
}

pub fn from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<Arc<dyn ObjectStorage>> {
    match Backend::from_vars(&vars)? {
        Backend::Gcs => Ok(Arc::new(GcsBackend::from_vars(&vars)?)),
        Backend::Memory => Ok(Arc::new(memory_from_vars(&vars))),
        Backend::Local => Ok(Arc::new(local_from_vars(&vars)?)),
        Backend::S3 => Ok(Arc::new(S3Backend::from_vars(&vars)?)),
    }
}

pub fn optional_from_vars(
    vars: impl Fn(&str) -> Option<String>,
) -> Result<Option<Arc<dyn ObjectStorage>>> {
    match Backend::from_vars(&vars)? {
        Backend::Gcs => Ok(Some(Arc::new(
            GcsBackend::from_vars(&vars).wrap_err("GCS backend configuration failed")?,
        ))),
        Backend::Memory => Ok(Some(Arc::new(memory_from_vars(&vars)))),
        Backend::Local => Ok(Some(Arc::new(local_from_vars(&vars)?))),
        Backend::S3 if S3Backend::env_is_complete(&vars) => {
            Ok(Some(Arc::new(S3Backend::from_vars(&vars)?)))
        }
        Backend::S3 => Ok(None),
    }
}

fn primary_bucket_from_vars(vars: impl Fn(&str) -> Option<String>) -> String {
    vars(ENV_BUCKET).unwrap_or_else(|| DEFAULT_BUCKET.into())
}

fn memory_from_vars(vars: impl Fn(&str) -> Option<String>) -> PrefixedBackend {
    PrefixedBackend::memory(primary_bucket_from_vars(vars))
}

fn local_from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<PrefixedBackend> {
    let dir = vars(ENV_LOCAL_DIR)
        .ok_or_else(|| eyre::eyre!("{ENV_BACKEND}=local requires {ENV_LOCAL_DIR}"))?;
    PrefixedBackend::local(Path::new(&dir), primary_bucket_from_vars(vars))
}

pub fn in_memory(bucket: &str) -> Arc<dyn ObjectStorage> {
    Arc::new(PrefixedBackend::memory(bucket.to_string()))
}

pub fn local_dir(dir: &Path, bucket: &str) -> Result<Arc<dyn ObjectStorage>> {
    Ok(Arc::new(PrefixedBackend::local(dir, bucket.to_string())?))
}

fn unknown_backend(got: &str) -> eyre::Report {
    eyre::eyre!("Unknown {ENV_BACKEND} '{got}' (expected 's3', 'gcs', 'memory' or 'local')")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_env::with as with_env;
    use cufflink_settings::process_var as var;

    #[test]
    fn unknown_backend_is_rejected_with_actionable_error() {
        with_env(&[(ENV_BACKEND, Some("azure"))], || {
            let err = from_vars(var).err().expect("unknown backend must error");
            let msg = err.to_string();
            assert!(msg.contains("azure"));
            assert!(msg.contains("s3"));
            assert!(msg.contains("gcs"));
        });
    }

    #[test]
    fn backend_names_parse_and_only_memory_and_local_are_ephemeral() {
        for backend in [Backend::S3, Backend::Gcs, Backend::Memory, Backend::Local] {
            assert_eq!(Backend::parse(Some(backend.name())).unwrap(), backend);
        }
        assert_eq!(Backend::parse(None).unwrap(), Backend::S3);
        assert!(Backend::parse(Some("azure")).is_err());
        assert!(Backend::Memory.is_ephemeral() && Backend::Local.is_ephemeral());
        assert!(!Backend::S3.is_ephemeral() && !Backend::Gcs.is_ephemeral());
    }

    #[test]
    fn memory_backend_is_selected_by_name() {
        with_env(
            &[
                (ENV_BACKEND, Some("memory")),
                (ENV_BUCKET, Some("test-bucket")),
            ],
            || {
                let store = optional_from_vars(var)
                    .expect("memory backend builds")
                    .expect("memory backend is configured");
                assert_eq!(store.backend_kind(), "memory");
                assert_eq!(store.primary_bucket(), "test-bucket");
                assert_eq!(
                    from_vars(var)
                        .expect("memory backend builds")
                        .backend_kind(),
                    "memory"
                );
            },
        );
    }

    #[test]
    fn local_backend_needs_its_directory() {
        with_env(
            &[(ENV_BACKEND, Some("local")), (ENV_LOCAL_DIR, None)],
            || {
                let err = optional_from_vars(var).err().expect("missing directory");
                assert!(err.to_string().contains(ENV_LOCAL_DIR), "{err}");
            },
        );
        let dir = std::env::temp_dir().join(format!("cufflink-storage-env-{}", std::process::id()));
        with_env(
            &[
                (ENV_BACKEND, Some("local")),
                (ENV_LOCAL_DIR, dir.to_str()),
                (ENV_BUCKET, Some("b")),
            ],
            || {
                let store = optional_from_vars(var)
                    .expect("local backend builds")
                    .expect("local backend is configured");
                assert_eq!(store.backend_kind(), "local");
                assert_eq!(store.primary_bucket(), "b");
            },
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn optional_returns_none_when_s3_env_incomplete() {
        with_env(
            &[
                (ENV_BACKEND, None),
                ("S3_ENDPOINT", None),
                ("S3_REGION", None),
                ("S3_ACCESS_KEY", None),
                ("S3_SECRET_KEY", None),
                ("S3_BUCKET", None),
            ],
            || {
                let result = optional_from_vars(var).expect("ok when unconfigured");
                assert!(result.is_none());
            },
        );
    }

    #[test]
    fn optional_builds_s3_when_all_vars_present() {
        with_env(
            &[
                (ENV_BACKEND, None),
                ("S3_ENDPOINT", Some("http://minio:9000")),
                ("S3_REGION", Some("us-east-1")),
                ("S3_ACCESS_KEY", Some("ak")),
                ("S3_SECRET_KEY", Some("sk")),
                ("S3_BUCKET", Some("cufflink")),
            ],
            || {
                let store = optional_from_vars(var)
                    .expect("ok")
                    .expect("should build s3");
                assert_eq!(store.backend_kind(), "s3");
                assert_eq!(store.primary_bucket(), "cufflink");
            },
        );
    }

    #[test]
    fn keys_given_as_files_configure_s3() {
        let settings = |name: &str| match name {
            "S3_ENDPOINT" => Some("http://minio:9000".to_string()),
            "S3_REGION" => Some("us-east-1".to_string()),
            "S3_BUCKET" => Some("cufflink".to_string()),
            "S3_ACCESS_KEY_FILE" => Some("/run/secrets/s3-access".to_string()),
            "S3_SECRET_KEY_FILE" => Some("/run/secrets/s3-secret".to_string()),
            _ => None,
        };
        assert!(
            optional_from_vars(settings).expect("ok").is_none(),
            "the file forms alone are not the keys"
        );
        let vars = cufflink_settings::with_secret_files(settings, |path| {
            Ok(format!("{}\n", path.display()).into_bytes())
        })
        .expect("secret files");
        let store = optional_from_vars(&vars)
            .expect("ok")
            .expect("s3 from files");
        assert_eq!(store.backend_kind(), "s3");
        assert_eq!(
            from_vars(&vars).expect("s3 from files").primary_bucket(),
            "cufflink"
        );
    }

    #[test]
    fn optional_builds_gcs_when_backend_is_gcs() {
        with_env(
            &[
                (ENV_BACKEND, Some("gcs")),
                ("GCS_BUCKET", Some("app-data")),
                ("GOOGLE_APPLICATION_CREDENTIALS", None),
            ],
            || {
                let store = optional_from_vars(var)
                    .expect("ok")
                    .expect("should build gcs");
                assert_eq!(store.backend_kind(), "gcs");
                assert_eq!(store.primary_bucket(), "app-data");
            },
        );
    }

    #[test]
    fn required_from_env_defaults_to_s3() {
        with_env(
            &[
                (ENV_BACKEND, None),
                ("S3_ENDPOINT", Some("http://minio:9000")),
                ("S3_REGION", Some("us-east-1")),
                ("S3_ACCESS_KEY", Some("ak")),
                ("S3_SECRET_KEY", Some("sk")),
                ("S3_BUCKET", Some("cufflink")),
            ],
            || {
                let store = from_vars(var).expect("should build s3 by default");
                assert_eq!(store.backend_kind(), "s3");
            },
        );
    }
}
