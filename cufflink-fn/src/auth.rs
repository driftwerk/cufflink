use cufflink_types::grants::{self, covers, parse_scoped};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Which rows a grant reaches on a table that opts into scoped permissions.
pub use cufflink_types::grants::Scope as GrantScope;

/// Authenticated user context, validated by the Cufflink platform.
///
/// The platform validates the JWT token (via Keycloak) and extracts claims
/// before passing them to your handler. You never need to validate tokens
/// yourself — the `auth` field is only present when the token is valid.
///
/// ```rust,ignore
/// handler!(protected, |req: Request| {
///     let auth = match req.require_auth() {
///         Ok(auth) => auth,
///         Err(resp) => return resp,
///     };
///     if !auth.has_role("admin") {
///         return Response::error("Forbidden");
///     }
///     Response::json(&json!({"user": auth.sub}))
/// });
/// ```
///
/// Tests that build an `Auth` should end the literal with
/// `..Default::default()`, so fields added in later releases do not break
/// them.
#[derive(Debug, Clone, Default)]
pub struct Auth {
    /// Keycloak subject ID (unique user identifier).
    pub sub: String,
    /// Preferred username from Keycloak.
    pub preferred_username: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Email address.
    pub email: Option<String>,
    /// Realm roles assigned to the user in Keycloak.
    pub realm_roles: Vec<String>,
    /// All other JWT claims (custom Keycloak mappers, resource_access, etc.).
    pub claims: HashMap<String, Value>,
    /// Cufflink permissions resolved from the service's tenant roles (e.g., `["staff:create", "items:*"]`).
    pub permissions: Vec<String>,
    /// Cufflink role names assigned to the user (e.g., `["admin", "manager"]`).
    pub role_names: Vec<String>,
    /// The caller's scoped grants verbatim (e.g., `["books:view@all"]`); each
    /// also appears in `permissions` as its unscoped base.
    pub scoped_permissions: Vec<String>,
    /// Whether this is a Keycloak service account (client credentials grant).
    /// Service accounts bypass permission checks at the platform level.
    pub is_service_account: bool,
}

impl Auth {
    /// The `auth` object of the platform's request JSON.
    pub(crate) fn from_json(auth: &Map<String, Value>) -> Self {
        let text = |key: &str| auth.get(key).and_then(Value::as_str).map(str::to_string);
        let strings = |key: &str| -> Vec<String> {
            auth.get(key)
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        Auth {
            sub: text("sub").unwrap_or_default(),
            preferred_username: text("preferred_username"),
            name: text("name"),
            email: text("email"),
            realm_roles: strings("realm_roles"),
            claims: auth
                .get("claims")
                .and_then(Value::as_object)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
            permissions: strings("permissions"),
            role_names: strings("role_names"),
            scoped_permissions: strings("scoped_permissions"),
            is_service_account: auth
                .get("is_service_account")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    /// Check if the user has a specific Keycloak realm role.
    pub fn has_role(&self, role: &str) -> bool {
        self.realm_roles.iter().any(|r| r == role)
    }

    /// Check if the user has a specific Cufflink permission.
    ///
    /// Supports wildcards: `"staff:*"` matches any operation in the "staff" area,
    /// and `"*"` matches everything.
    ///
    /// ```rust,ignore
    /// if !auth.can("staff", "create") {
    ///     return Response::error("Forbidden: missing staff:create permission");
    /// }
    /// ```
    pub fn can(&self, area: &str, operation: &str) -> bool {
        self.permissions.iter().any(|p| covers(p, area, operation))
    }

    /// The widest scope at which the caller holds `{area}:{operation}`: `All`,
    /// else the first `Field` grant, else `Own` (an unscoped grant reaches the
    /// caller's own rows on a scoped table); `None` without the permission.
    ///
    /// This is one grant, where the platform's CRUD routes reach the rows of
    /// every `Own` and `Field` grant together, so a guest applying it sees
    /// at most what the platform would show. A route guarded only by
    /// [`Auth::can`] ignores scopes altogether: use this (or a `scoped(...)`
    /// posture) wherever rows must be narrowed.
    ///
    /// ```rust,ignore
    /// use cufflink_fn::GrantScope;
    ///
    /// match auth.scope_for("books", "view") {
    ///     Some(GrantScope::All) => list_all(),
    ///     Some(GrantScope::Field { column, claim }) => list_where(&column, auth.claim(&claim)),
    ///     Some(GrantScope::Own) => list_where("user_id", Some(&json!(auth.sub))),
    ///     None => Response::forbidden("Missing permission: books:view"),
    /// }
    /// ```
    pub fn scope_for(&self, area: &str, operation: &str) -> Option<GrantScope> {
        self.scoped_permissions
            .iter()
            .filter_map(|p| parse_scoped(p))
            .filter(|grant| grant.covers(area, operation))
            .map(|grant| grant.scope)
            .min_by_key(|scope| std::cmp::Reverse(breadth(scope)))
            .or_else(|| self.can(area, operation).then_some(GrantScope::Own))
    }

    /// Check if the user has a specific Cufflink role (by name).
    pub fn has_cufflink_role(&self, role: &str) -> bool {
        self.role_names.iter().any(|r| r == role)
    }

    /// A claim by name: the claim of exactly that name, else the dotted
    /// path through nested claims (`org.id` reads `{"org": {"id": …}}`), as
    /// the platform resolves a `@field` grant's claim.
    pub fn claim(&self, key: &str) -> Option<&Value> {
        grants::claim(&self.claims, key)
    }
}

fn breadth(scope: &GrantScope) -> u8 {
    match scope {
        GrantScope::Own => 0,
        GrantScope::Field { .. } => 1,
        GrantScope::All => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;
    use serde_json::json;

    #[test]
    fn test_auth_permissions() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "test",
            "headers": {},
            "body": {},
            "tenant": "acme",
            "service": "shop",
            "auth": {
                "sub": "user-1",
                "realm_roles": ["admin"],
                "claims": {},
                "permissions": ["staff:create", "staff:view", "items:*"],
                "role_names": ["admin", "manager"]
            }
        }))
        .unwrap();

        let req = Request::from_json(&json).unwrap();
        let auth = req.auth().unwrap();

        assert!(auth.can("staff", "create"));
        assert!(auth.can("staff", "view"));
        assert!(!auth.can("staff", "delete"));

        assert!(auth.can("items", "create"));
        assert!(auth.can("items", "view"));
        assert!(auth.can("items", "delete"));

        assert!(!auth.can("batches", "view"));

        assert!(auth.has_cufflink_role("admin"));
        assert!(auth.has_cufflink_role("manager"));
        assert!(!auth.has_cufflink_role("viewer"));
    }

    #[test]
    fn test_auth_super_wildcard() {
        let auth = Auth {
            sub: "user-1".to_string(),
            permissions: vec!["*".to_string()],
            role_names: vec!["superadmin".to_string()],
            ..Default::default()
        };

        assert!(auth.can("anything", "everything"));
        assert!(auth.can("staff", "create"));
    }

    #[test]
    fn test_auth_empty_permissions() {
        let auth = Auth {
            sub: "user-1".to_string(),
            ..Default::default()
        };

        assert!(!auth.can("staff", "create"));
        assert!(!auth.has_cufflink_role("admin"));
    }

    fn scoped(permissions: &[&str], scoped: &[&str]) -> Auth {
        let json = serde_json::to_string(&json!({
            "method": "GET",
            "handler": "h",
            "headers": {},
            "body": {},
            "tenant": "acme",
            "service": "crm",
            "auth": {
                "sub": "user-1",
                "claims": {"branch_id": "branch-1", "org": {"id": "org-2"}},
                "permissions": permissions,
                "scoped_permissions": scoped,
            }
        }))
        .unwrap();
        Request::from_json(&json).unwrap().auth().unwrap().clone()
    }

    #[test]
    fn scope_for_returns_the_widest_matching_grant() {
        let branch = GrantScope::Field {
            column: "branch_id".into(),
            claim: "branch_id".into(),
        };
        let auth = scoped(
            &["books:view", "books:edit", "receipts:view"],
            &[
                "books:view@field(branch_id=branch_id)",
                "books:view@own",
                "books:edit@field(branch_id=branch_id)",
                "books:edit@field(team_id=team_id)",
                "receipts:*@all",
            ],
        );
        assert_eq!(auth.scope_for("books", "view"), Some(branch.clone()));
        assert_eq!(auth.scope_for("books", "edit"), Some(branch));
        assert_eq!(auth.scope_for("receipts", "view"), Some(GrantScope::All));
        assert_eq!(auth.scope_for("books", "delete"), None);
        assert_eq!(auth.claim("branch_id"), Some(&json!("branch-1")));
        assert_eq!(auth.claim("org.id"), Some(&json!("org-2")));
        assert_eq!(auth.claim("org.name"), None);
    }

    #[test]
    fn an_unscoped_grant_reads_as_own_and_old_payloads_have_no_scopes() {
        let auth = scoped(&["books:view", "*"], &[]);
        assert_eq!(auth.scope_for("books", "view"), Some(GrantScope::Own));
        assert_eq!(auth.scope_for("anything", "at_all"), Some(GrantScope::Own));
        assert!(auth.scoped_permissions.is_empty());
        assert_eq!(scoped(&[], &[]).scope_for("books", "view"), None);
    }
}
