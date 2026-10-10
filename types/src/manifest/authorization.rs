use serde::{Deserialize, Serialize};

/// Authorization configuration declared in the service manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationConfig {
    /// Permission areas declared by this service.
    #[serde(default)]
    pub areas: Vec<PermissionAreaDef>,
    /// Default roles to seed on first deployment.
    #[serde(default)]
    pub default_roles: Vec<DefaultRoleDef>,
}

/// A permission area with its supported operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionAreaDef {
    pub name: String,
    #[serde(default)]
    pub operations: Vec<String>,
}

/// A default role seeded on deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultRoleDef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub permissions: Vec<String>,
}
