//! SCIM 2.0 provisioning (1.12): an identity provider (Microsoft Entra
//! ID, Okta, Keycloak with a SCIM extension, ...) tells the cluster who
//! its people and groups are, so someone removed or turned off there
//! loses their sessions at once instead of when they expire.
//!
//! The provider talks HTTP to `pumasi ui` at `<public URL>/scim/v2` with
//! a bearer token from `pumasi sso scim token create`. The dashboard
//! passes each request through unchanged ([`ScimRequest`]); the control
//! plane checks the token, keeps the users and groups in its state
//! database (replicated like everything else) and answers in SCIM's JSON.

use serde::{Deserialize, Serialize};

/// The path the dashboard serves SCIM under.
pub const SCIM_PREFIX: &str = "/scim/v2";
/// SCIM tokens start with this, so a leaked one is easy to search for.
pub const TOKEN_PREFIX: &str = "pumasi-scim-";
/// Largest SCIM request body.
pub const MAX_BODY: usize = 1 << 20;

/// One HTTP request to `/scim/v2/...`, as the dashboard received it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimRequest {
    /// The bearer token from the `Authorization` header (empty: none).
    pub token: crate::sso::Hidden,
    /// `GET`, `POST`, `PUT`, `PATCH` or `DELETE`.
    pub method: String,
    /// The path after `/scim/v2`, e.g. `/Users/2819c223`.
    pub path: String,
    /// The query string without `?` (`filter=...&startIndex=1`).
    pub query: String,
    pub body: String,
    /// `<public URL>/scim/v2`, for the `location` of resources.
    pub base_url: String,
}

/// The answer: an HTTP status and a JSON body (empty for 204).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimResponse {
    pub status: u16,
    pub body: String,
}

/// A SCIM token as admins see it (never its value after creation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimTokenView {
    pub id: String,
    pub label: String,
    pub created_unix: u64,
    /// 0 when it was never used.
    pub last_used_unix: u64,
}

/// A provisioned person, for `pumasi sso scim users`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimUserView {
    pub id: String,
    pub user_name: String,
    pub external_id: Option<String>,
    pub display_name: Option<String>,
    pub emails: Vec<String>,
    pub active: bool,
    /// The display names of the SCIM groups they are in.
    pub groups: Vec<String>,
    /// The access tickets (sessions) of theirs that are live now.
    pub sessions: u32,
    pub updated_unix: u64,
}

/// A provisioned group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimGroupView {
    pub id: String,
    pub display_name: String,
    pub external_id: Option<String>,
    pub members: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimView {
    pub tokens: Vec<ScimTokenView>,
    pub users: Vec<ScimUserView>,
    pub groups: Vec<ScimGroupView>,
}
