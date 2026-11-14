//! SCIM 2.0 provisioning (1.12; RFC 7643 and 7644): the users and groups
//! an identity provider pushes, and what follows from them. See
//! [`pumasi_proto::scim`] for how requests get here.
//!
//! What it does to the cluster:
//!
//! - A person deleted, or set `active: false`, loses every SSO session
//!   (access tickets named `sso:<user name or email>` or `ldap:<user
//!   name>`) at once, and their dashboard sessions with them; while they
//!   are inactive, signing in is refused even if the provider still
//!   answers for them.
//! - A person removed from a group that a sign-in rule names (by its
//!   display name, id or external id), or a whole such group deleted,
//!   loses their sessions too: their role came from it. They sign in
//!   again and get the role the rules give them now.
//!
//! Everyone else is unaffected: SCIM adds to SSO, it does not replace it.
//! People appear on their first sign-in as before; provisioning someone
//! does not create a ticket.
//!
//! Only what the cluster uses is kept: user name, external id, names,
//! emails, `active`, and group names and members. Other attributes a
//! provider sends (titles, phone numbers, the enterprise extension) are
//! accepted and dropped, so provisioning does not fail on them.

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use pumasi_proto::{
    Secret,
    scim::{
        ScimGroupView, ScimRequest, ScimResponse, ScimTokenView, ScimUserView, ScimView,
        TOKEN_PREFIX,
    },
    sso::{Identity, Subject},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tracing::info;

use crate::{
    audit::Actor,
    sso::is_sso_name,
    state::{State, now_unix},
};

const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
const GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
const LIST_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:ListResponse";
const ERROR_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:Error";
const ENTERPRISE: &str = "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User";

/// Tokens at most.
const MAX_TOKENS: usize = 20;
/// Users and groups at most (each).
pub const MAX_RESOURCES: usize = 100_000;
/// Members of one group at most.
const MAX_MEMBERS: usize = 100_000;
/// A page of a listing at most.
const MAX_PAGE: usize = 1000;
/// Longest string attribute kept.
const MAX_STR: usize = 512;

/// A SCIM token as stored: its hash, never its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimTokenRecord {
    pub id: String,
    pub label: String,
    pub hash: [u8; 32],
    pub created_unix: u64,
    pub last_used_unix: u64,
}

impl ScimTokenRecord {
    fn view(&self) -> ScimTokenView {
        ScimTokenView {
            id: self.id.clone(),
            label: self.label.clone(),
            created_unix: self.created_unix,
            last_used_unix: self.last_used_unix,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimEmail {
    pub value: String,
    pub kind: Option<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimUser {
    pub id: String,
    pub user_name: String,
    pub external_id: Option<String>,
    pub display_name: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    pub formatted_name: Option<String>,
    pub emails: Vec<ScimEmail>,
    pub active: bool,
    pub created_unix: u64,
    pub updated_unix: u64,
    pub version: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScimGroup {
    pub id: String,
    pub display_name: String,
    pub external_id: Option<String>,
    /// User ids.
    pub members: BTreeSet<String>,
    pub created_unix: u64,
    pub updated_unix: u64,
    pub version: u64,
}

/// In memory on the leader, loaded from the state database.
#[derive(Debug, Default)]
pub(crate) struct ScimState {
    pub tokens: Vec<ScimTokenRecord>,
    pub users: std::collections::BTreeMap<String, ScimUser>,
    pub groups: std::collections::BTreeMap<String, ScimGroup>,
}

fn token_hash(token: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/scim-token/v1\0");
    h.update(token.as_bytes());
    *h.finalize().as_bytes()
}

/// A SCIM error response.
#[derive(Debug)]
struct ScimError {
    status: u16,
    scim_type: Option<&'static str>,
    detail: String,
}

fn err(status: u16, scim_type: Option<&'static str>, detail: impl Into<String>) -> ScimError {
    ScimError {
        status,
        scim_type,
        detail: detail.into(),
    }
}

fn bad(detail: impl Into<String>) -> ScimError {
    err(400, Some("invalidValue"), detail)
}

fn not_found(what: &str, id: &str) -> ScimError {
    err(404, None, format!("no {what} {id}"))
}

impl ScimError {
    fn response(&self) -> ScimResponse {
        let mut body = json!({
            "schemas": [ERROR_SCHEMA],
            "status": self.status.to_string(),
            "detail": self.detail,
        });
        if let Some(t) = self.scim_type {
            body["scimType"] = json!(t);
        }
        ScimResponse {
            status: self.status,
            body: body.to_string(),
        }
    }
}

type R<T> = std::result::Result<T, ScimError>;

fn ok(status: u16, body: Value) -> ScimResponse {
    ScimResponse {
        status,
        body: body.to_string(),
    }
}

/// A string attribute, cut to a sane length.
fn clip(s: &str) -> String {
    let mut s = s.trim().to_string();
    if s.len() > MAX_STR {
        let mut cut = MAX_STR;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    s.chars().filter(|c| !c.is_control()).collect()
}

fn opt_str(v: &Value) -> R<Option<String>> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) if s.trim().is_empty() => Ok(None),
        Value::String(s) => Ok(Some(clip(s))),
        other => Err(bad(format!("expected a string, got {other}"))),
    }
}

/// SCIM booleans, and the strings Entra ID sends for them ("True").
fn as_bool(v: &Value) -> R<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(true),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(false),
        other => Err(bad(format!("expected true or false, got {other}"))),
    }
}

fn emails_from(v: &Value) -> R<Vec<ScimEmail>> {
    let Value::Array(items) = v else {
        if v.is_null() {
            return Ok(Vec::new());
        }
        return Err(bad("emails is a list"));
    };
    let mut out = Vec::new();
    for item in items.iter().take(20) {
        let Some(value) = item.get("value").and_then(Value::as_str) else {
            continue;
        };
        if value.trim().is_empty() {
            continue;
        }
        out.push(ScimEmail {
            value: clip(value),
            kind: item.get("type").and_then(Value::as_str).map(clip),
            primary: item
                .get("primary")
                .map(as_bool)
                .transpose()?
                .unwrap_or(false),
        });
    }
    Ok(out)
}

/// Looks an attribute up by name without case, as SCIM wants.
fn get_ci<'a>(obj: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    obj.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

impl ScimUser {
    /// Sets the attributes a full representation gives (POST and PUT).
    fn replace_from(&mut self, body: &Map<String, Value>) -> R<()> {
        let user_name = get_ci(body, "userName")
            .map(opt_str)
            .transpose()?
            .flatten()
            .ok_or_else(|| bad("userName is required"))?;
        self.user_name = user_name;
        self.external_id = get_ci(body, "externalId")
            .map(opt_str)
            .transpose()?
            .flatten();
        self.display_name = get_ci(body, "displayName")
            .map(opt_str)
            .transpose()?
            .flatten();
        let name = get_ci(body, "name").and_then(Value::as_object);
        self.given_name = name
            .and_then(|n| get_ci(n, "givenName"))
            .map(opt_str)
            .transpose()?
            .flatten();
        self.family_name = name
            .and_then(|n| get_ci(n, "familyName"))
            .map(opt_str)
            .transpose()?
            .flatten();
        self.formatted_name = name
            .and_then(|n| get_ci(n, "formatted"))
            .map(opt_str)
            .transpose()?
            .flatten();
        self.emails = get_ci(body, "emails")
            .map(emails_from)
            .transpose()?
            .unwrap_or_default();
        self.active = get_ci(body, "active")
            .map(as_bool)
            .transpose()?
            .unwrap_or(true);
        Ok(())
    }

    /// One attribute set by a PATCH (`path` already lowercased); unknown
    /// ones are dropped.
    fn set(&mut self, path: &str, value: &Value, remove: bool) -> R<()> {
        let s = || if remove { Ok(None) } else { opt_str(value) };
        match path {
            "username" => {
                self.user_name = s()?.ok_or_else(|| bad("userName cannot be removed"))?;
            }
            "externalid" => self.external_id = s()?,
            "displayname" => self.display_name = s()?,
            "name.givenname" => self.given_name = s()?,
            "name.familyname" => self.family_name = s()?,
            "name.formatted" => self.formatted_name = s()?,
            "name" => {
                if remove {
                    self.given_name = None;
                    self.family_name = None;
                    self.formatted_name = None;
                } else if let Some(n) = value.as_object() {
                    for (k, v) in n {
                        self.set(&format!("name.{}", k.to_ascii_lowercase()), v, false)?;
                    }
                }
            }
            "active" => {
                self.active = if remove { false } else { as_bool(value)? };
            }
            "emails" => {
                self.emails = if remove {
                    Vec::new()
                } else {
                    emails_from(value)?
                };
            }
            p if p.starts_with("emails[") => {
                // `emails[type eq "work"].value` (Entra ID).
                let kind = p
                    .split('"')
                    .nth(1)
                    .map(str::to_string)
                    .unwrap_or_else(|| "work".into());
                self.emails.retain(|e| {
                    !e.kind
                        .as_deref()
                        .is_some_and(|k| k.eq_ignore_ascii_case(&kind))
                });
                if !remove && let Some(v) = opt_str(value)? {
                    let primary = self.emails.is_empty();
                    self.emails.push(ScimEmail {
                        value: v,
                        kind: Some(kind),
                        primary,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn json(&self, base: &str, groups: &[(&String, &String)]) -> Value {
        let mut v = json!({
            "schemas": [USER_SCHEMA],
            "id": self.id,
            "userName": self.user_name,
            "active": self.active,
            "meta": meta("User", &format!("{base}/Users/{}", self.id), self.created_unix, self.updated_unix, self.version),
        });
        if let Some(x) = &self.external_id {
            v["externalId"] = json!(x);
        }
        if let Some(x) = &self.display_name {
            v["displayName"] = json!(x);
        }
        let mut name = Map::new();
        for (k, x) in [
            ("givenName", &self.given_name),
            ("familyName", &self.family_name),
            ("formatted", &self.formatted_name),
        ] {
            if let Some(x) = x {
                name.insert(k.into(), json!(x));
            }
        }
        if !name.is_empty() {
            v["name"] = Value::Object(name);
        }
        if !self.emails.is_empty() {
            v["emails"] = Value::Array(
                self.emails
                    .iter()
                    .map(|e| {
                        let mut m = json!({"value": e.value, "primary": e.primary});
                        if let Some(k) = &e.kind {
                            m["type"] = json!(k);
                        }
                        m
                    })
                    .collect(),
            );
        }
        if !groups.is_empty() {
            v["groups"] = Value::Array(
                groups
                    .iter()
                    .map(|(id, name)| json!({"value": id, "display": name, "$ref": format!("{base}/Groups/{id}")}))
                    .collect(),
            );
        }
        v
    }

    /// The access ticket names an SSO sign-in of this person gets.
    fn ticket_names(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let user = self.user_name.trim();
        out.insert(format!("sso:{}", user.to_lowercase()));
        out.insert(format!("ldap:{user}"));
        out.insert(format!("ldap:{}", user.to_lowercase()));
        for e in &self.emails {
            out.insert(format!("sso:{}", e.value.trim().to_lowercase()));
        }
        out
    }

    /// Whether a signing-in identity is this person.
    fn is(&self, who: &Identity) -> bool {
        if self.ticket_names().contains(&who.name)
            || self.ticket_names().contains(&who.name.to_lowercase())
        {
            return true;
        }
        who.verified_email.as_deref().is_some_and(|mail| {
            self.user_name.eq_ignore_ascii_case(mail)
                || self
                    .emails
                    .iter()
                    .any(|e| e.value.eq_ignore_ascii_case(mail))
        })
    }

    fn view(&self, groups: Vec<String>, sessions: u32) -> ScimUserView {
        ScimUserView {
            id: self.id.clone(),
            user_name: self.user_name.clone(),
            external_id: self.external_id.clone(),
            display_name: self.display_name.clone(),
            emails: self.emails.iter().map(|e| e.value.clone()).collect(),
            active: self.active,
            groups,
            sessions,
            updated_unix: self.updated_unix,
        }
    }
}

fn meta(kind: &str, location: &str, created: u64, updated: u64, version: u64) -> Value {
    json!({
        "resourceType": kind,
        "created": pumasi_proto::audit::rfc3339(created),
        "lastModified": pumasi_proto::audit::rfc3339(updated),
        "location": location,
        "version": format!("W/\"{version}\""),
    })
}

/// Member ids from a `members` value: `[{"value": "id"}, ...]`.
fn member_ids(v: &Value) -> R<Vec<String>> {
    match v {
        Value::Null => Ok(Vec::new()),
        Value::Array(items) => Ok(items
            .iter()
            .filter_map(|m| m.get("value").and_then(Value::as_str))
            .map(clip)
            .filter(|s| !s.is_empty())
            .collect()),
        Value::Object(_) => member_ids(&Value::Array(vec![v.clone()])),
        other => Err(bad(format!("members is a list, got {other}"))),
    }
}

impl ScimGroup {
    fn json(
        &self,
        base: &str,
        users: &std::collections::BTreeMap<String, ScimUser>,
        with_members: bool,
    ) -> Value {
        let mut v = json!({
            "schemas": [GROUP_SCHEMA],
            "id": self.id,
            "displayName": self.display_name,
            "meta": meta("Group", &format!("{base}/Groups/{}", self.id), self.created_unix, self.updated_unix, self.version),
        });
        if let Some(x) = &self.external_id {
            v["externalId"] = json!(x);
        }
        if with_members {
            v["members"] = Value::Array(
                self.members
                    .iter()
                    .map(|id| {
                        let mut m = json!({"value": id, "$ref": format!("{base}/Users/{id}")});
                        if let Some(u) = users.get(id) {
                            m["display"] = json!(
                                u.display_name
                                    .clone()
                                    .unwrap_or_else(|| u.user_name.clone())
                            );
                        }
                        m
                    })
                    .collect(),
            );
        }
        v
    }

    /// Whether a sign-in rule names this group.
    fn named_by(&self, subject: &Subject) -> bool {
        let Subject::Group(g) = subject else {
            return false;
        };
        let g = pumasi_proto::sso::normalize_dn(g);
        [
            Some(&self.display_name),
            Some(&self.id),
            self.external_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|n| pumasi_proto::sso::normalize_dn(n) == g)
    }
}

// ---------------------------------------------------------------------------
// Filters: `attr eq "value"`, joined with `and`. That is what Entra ID,
// Okta and the Keycloak extensions send.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
struct Cond {
    attr: String,
    value: String,
}

fn parse_filter(f: &str) -> R<Vec<Cond>> {
    let mut out = Vec::new();
    let mut rest = f.trim();
    loop {
        let (attr, after) = rest.split_once(char::is_whitespace).ok_or_else(|| {
            err(
                400,
                Some("invalidFilter"),
                format!("cannot read filter {f:?}"),
            )
        })?;
        let after = after.trim_start();
        let (op, after) = after.split_once(char::is_whitespace).ok_or_else(|| {
            err(
                400,
                Some("invalidFilter"),
                format!("cannot read filter {f:?}"),
            )
        })?;
        if !op.eq_ignore_ascii_case("eq") {
            return Err(err(
                400,
                Some("invalidFilter"),
                format!("only `eq` filters are supported, not {op:?}"),
            ));
        }
        let after = after.trim_start();
        let (value, after) = if let Some(q) = after.strip_prefix('"') {
            // A quoted string with `\"` escapes.
            let mut value = String::new();
            let mut chars = q.char_indices();
            let mut end = None;
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' => {
                        if let Some((_, n)) = chars.next() {
                            value.push(n);
                        }
                    }
                    '"' => {
                        end = Some(i + 1);
                        break;
                    }
                    c => value.push(c),
                }
            }
            let end = end
                .ok_or_else(|| err(400, Some("invalidFilter"), "unterminated string in filter"))?;
            (value, &q[end..])
        } else {
            let (v, a) = after.split_once(char::is_whitespace).unwrap_or((after, ""));
            (v.to_string(), a)
        };
        out.push(Cond {
            attr: attr.to_ascii_lowercase(),
            value,
        });
        let after = after.trim();
        if after.is_empty() {
            return Ok(out);
        }
        let Some(next) = after
            .strip_prefix("and ")
            .or_else(|| after.strip_prefix("AND "))
            .or_else(|| after.strip_prefix("And "))
        else {
            return Err(err(
                400,
                Some("invalidFilter"),
                format!("only `and` joins filters, near {after:?}"),
            ));
        };
        rest = next.trim_start();
        if out.len() > 8 {
            return Err(err(400, Some("invalidFilter"), "filter too long"));
        }
    }
}

fn user_matches(u: &ScimUser, c: &Cond) -> bool {
    let eq = |s: &str| s.eq_ignore_ascii_case(&c.value);
    match c.attr.as_str() {
        "id" => u.id == c.value,
        "username" => eq(&u.user_name),
        "externalid" => u.external_id.as_deref() == Some(c.value.as_str()),
        "displayname" => u.display_name.as_deref().is_some_and(eq),
        "active" => as_bool(&Value::String(c.value.clone())).is_ok_and(|b| b == u.active),
        a if a == "emails" || a.starts_with("emails.") || a.starts_with("emails[") => {
            u.emails.iter().any(|e| eq(&e.value))
        }
        _ => false,
    }
}

fn group_matches(g: &ScimGroup, c: &Cond) -> bool {
    match c.attr.as_str() {
        "id" => g.id == c.value,
        "displayname" => g.display_name.eq_ignore_ascii_case(&c.value),
        "externalid" => g.external_id.as_deref() == Some(c.value.as_str()),
        "members" | "members.value" => g.members.contains(&c.value),
        _ => false,
    }
}

/// `key=value&...`, percent-decoded.
fn query_params(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16)
                {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The PATCH operations of a body, as (op, lowercased path, value).
fn patch_ops(body: &Value) -> R<Vec<(String, Option<String>, Value)>> {
    let ops = body
        .get("Operations")
        .or_else(|| body.get("operations"))
        .and_then(Value::as_array)
        .ok_or_else(|| bad("a PATCH needs Operations"))?;
    let mut out = Vec::new();
    for o in ops.iter().take(1000) {
        let op = o
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("each operation needs an op"))?
            .to_ascii_lowercase();
        if !matches!(op.as_str(), "add" | "replace" | "remove") {
            return Err(bad(format!("unknown PATCH op {op:?}")));
        }
        let path = o.get("path").and_then(Value::as_str).map(|p| {
            // Paths may carry the core schema: `urn:...:User:active`.
            let p = p.trim();
            let p = p
                .strip_prefix(USER_SCHEMA)
                .or_else(|| p.strip_prefix(GROUP_SCHEMA))
                .map(|r| r.trim_start_matches(':'))
                .unwrap_or(p);
            p.to_string()
        });
        out.push((op, path, o.get("value").cloned().unwrap_or(Value::Null)));
    }
    Ok(out)
}

/// What a change does to sessions.
#[derive(Default)]
struct Fallout {
    /// Users whose sessions end, with why.
    end: Vec<(String, &'static str)>,
}

impl State {
    pub(crate) fn load_scim(&mut self) -> Result<()> {
        self.scim.tokens = self.store.scim_tokens()?;
        self.scim.users = self
            .store
            .scim_users()?
            .into_iter()
            .map(|u| (u.id.clone(), u))
            .collect();
        self.scim.groups = self
            .store
            .scim_groups()?
            .into_iter()
            .map(|g| (g.id.clone(), g))
            .collect();
        Ok(())
    }

    pub fn scim_token_create(&mut self, label: &str) -> Result<(String, ScimTokenView)> {
        let label: String = label
            .trim()
            .chars()
            .filter(|c| !c.is_control())
            .take(64)
            .collect();
        anyhow::ensure!(
            !label.is_empty(),
            "give the token a label (the identity provider's name)"
        );
        anyhow::ensure!(
            self.scim.tokens.len() < MAX_TOKENS,
            "at most {MAX_TOKENS} SCIM tokens; revoke one first"
        );
        let secret: Secret = pumasi_proto::new_secret();
        let token = format!(
            "{TOKEN_PREFIX}{}",
            data_encoding::BASE32_NOPAD.encode(&secret).to_lowercase()
        );
        let hash = token_hash(&token);
        let rec = ScimTokenRecord {
            id: data_encoding::HEXLOWER.encode(&hash[..6]),
            label,
            hash,
            created_unix: now_unix(),
            last_used_unix: 0,
        };
        let mut tokens = self.scim.tokens.clone();
        tokens.push(rec.clone());
        self.store.put_scim_tokens(&tokens)?;
        self.scim.tokens = tokens;
        Ok((token, rec.view()))
    }

    pub fn scim_token_revoke(&mut self, id: &str) -> Result<ScimTokenView> {
        let id = id.trim();
        anyhow::ensure!(!id.is_empty(), "which token?");
        let hits: Vec<usize> = (0..self.scim.tokens.len())
            .filter(|&i| self.scim.tokens[i].id.starts_with(id) || self.scim.tokens[i].label == id)
            .collect();
        match hits.as_slice() {
            [] => bail!("no SCIM token {id:?}"),
            [i] => {
                let mut tokens = self.scim.tokens.clone();
                let gone = tokens.remove(*i);
                self.store.put_scim_tokens(&tokens)?;
                self.scim.tokens = tokens;
                Ok(gone.view())
            }
            _ => bail!("{id:?} matches several tokens; give more of the id"),
        }
    }

    pub fn scim_view(&self) -> ScimView {
        ScimView {
            tokens: self.scim.tokens.iter().map(ScimTokenRecord::view).collect(),
            users: self
                .scim
                .users
                .values()
                .map(|u| {
                    let groups = self
                        .scim
                        .groups
                        .values()
                        .filter(|g| g.members.contains(&u.id))
                        .map(|g| g.display_name.clone())
                        .collect();
                    u.view(groups, self.live_sessions_of(u))
                })
                .collect(),
            groups: self
                .scim
                .groups
                .values()
                .map(|g| ScimGroupView {
                    id: g.id.clone(),
                    display_name: g.display_name.clone(),
                    external_id: g.external_id.clone(),
                    members: g.members.len() as u32,
                })
                .collect(),
        }
    }

    fn live_sessions_of(&self, u: &ScimUser) -> u32 {
        let names = u.ticket_names();
        let now = now_unix();
        self.mgmt
            .access
            .iter()
            .filter(|r| {
                is_sso_name(&r.name)
                    && names.contains(&r.name)
                    && r.expires_unix.is_none_or(|e| e > now)
            })
            .count() as u32
    }

    /// Refuses a sign-in of someone provisioned as inactive.
    pub(crate) fn scim_refuses(&self, who: &Identity) -> Option<String> {
        self.scim
            .users
            .values()
            .find(|u| !u.active && u.is(who))
            .map(|u| {
                format!(
                    "{} is turned off in the identity provider (SCIM user {})",
                    who.name, u.user_name
                )
            })
    }

    /// The token's record, if `token` is a live SCIM token.
    fn scim_token(&mut self, token: &str) -> Option<ScimTokenRecord> {
        if !token.starts_with(TOKEN_PREFIX) {
            return None;
        }
        let hash = token_hash(token);
        let i = self
            .scim
            .tokens
            .iter()
            .position(|t| pumasi_proto::secrets_eq(&t.hash, &hash))?;
        let now = now_unix();
        // Last use, to the hour: a provider calls every few minutes, and
        // each write is a Raft entry.
        if now.saturating_sub(self.scim.tokens[i].last_used_unix) >= 3600 {
            self.scim.tokens[i].last_used_unix = now;
            let _ = self.store.put_scim_tokens(&self.scim.tokens);
        }
        Some(self.scim.tokens[i].clone())
    }

    /// Answers one SCIM request. `Err` only for a missing or wrong token
    /// (the caller counts it against the peer); everything else is a
    /// SCIM response, errors included.
    pub fn scim_request(
        &mut self,
        req: &ScimRequest,
        peer: &str,
    ) -> std::result::Result<ScimResponse, ScimResponse> {
        let Some(token) = self.scim_token(req.token.expose()) else {
            return Err(err(
                401,
                None,
                "a SCIM bearer token is required (pumasi sso scim token create)",
            )
            .response());
        };
        let who = Actor {
            name: format!("scim:{}", token.label),
            role: "scim".into(),
            peer: peer.into(),
        };
        let mut fallout = Fallout::default();
        let res = self.scim_route(req, &who, &mut fallout);
        for (user_id, why) in fallout.end {
            self.scim_end_sessions(&user_id, why, &who);
        }
        Ok(res.unwrap_or_else(|e| e.response()))
    }

    fn scim_route(
        &mut self,
        req: &ScimRequest,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let base = req.base_url.trim_end_matches('/');
        let path = req.path.trim_end_matches('/');
        let mut parts = path.trim_start_matches('/').splitn(2, '/');
        let kind = parts.next().unwrap_or("");
        let id = parts.next().map(pct_decode);
        let method = req.method.to_ascii_uppercase();
        let body = || -> R<Value> {
            if req.body.trim().is_empty() {
                return Err(bad("a JSON body is required"));
            }
            serde_json::from_str(&req.body)
                .map_err(|e| err(400, Some("invalidSyntax"), format!("not JSON: {e}")))
        };
        match (method.as_str(), kind, id) {
            ("GET", "ServiceProviderConfig", None) => Ok(ok(200, service_provider_config(base))),
            ("GET", "ResourceTypes", None) => Ok(ok(200, resource_types(base))),
            ("GET", "Schemas", None) => Ok(ok(200, schemas())),
            ("GET", "Users", None) => self.scim_list_users(base, &req.query),
            ("GET", "Users", Some(id)) => {
                let u = self
                    .scim
                    .users
                    .get(&id)
                    .ok_or_else(|| not_found("user", &id))?;
                Ok(ok(200, u.json(base, &self.groups_of(&id))))
            }
            ("POST", "Users", None) => self.scim_create_user(base, &body()?, who),
            ("PUT", "Users", Some(id)) => self.scim_put_user(base, &id, &body()?, who, fallout),
            ("PATCH", "Users", Some(id)) => self.scim_patch_user(base, &id, &body()?, who, fallout),
            ("DELETE", "Users", Some(id)) => self.scim_delete_user(&id, who, fallout),
            ("GET", "Groups", None) => self.scim_list_groups(base, &req.query),
            ("GET", "Groups", Some(id)) => {
                let g = self
                    .scim
                    .groups
                    .get(&id)
                    .ok_or_else(|| not_found("group", &id))?;
                let members = !excluded_members(&req.query);
                Ok(ok(200, g.json(base, &self.scim.users, members)))
            }
            ("POST", "Groups", None) => self.scim_create_group(base, &body()?, who),
            ("PUT", "Groups", Some(id)) => self.scim_put_group(base, &id, &body()?, who, fallout),
            ("PATCH", "Groups", Some(id)) => {
                self.scim_patch_group(base, &id, &body()?, who, fallout)
            }
            ("DELETE", "Groups", Some(id)) => self.scim_delete_group(&id, who, fallout),
            (_, "Users" | "Groups" | "ServiceProviderConfig" | "ResourceTypes" | "Schemas", _) => {
                Err(err(405, None, format!("{method} is not supported here")))
            }
            _ => Err(err(404, None, format!("no SCIM endpoint {path}"))),
        }
    }

    fn groups_of(&self, user_id: &str) -> Vec<(&String, &String)> {
        self.scim
            .groups
            .values()
            .filter(|g| g.members.contains(user_id))
            .map(|g| (&g.id, &g.display_name))
            .collect()
    }

    fn scim_list_users(&self, base: &str, query: &str) -> R<ScimResponse> {
        let params = query_params(query);
        let filter = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("filter"));
        let conds = filter
            .map(|(_, f)| parse_filter(f))
            .transpose()?
            .unwrap_or_default();
        let all: Vec<&ScimUser> = self
            .scim
            .users
            .values()
            .filter(|u| conds.iter().all(|c| user_matches(u, c)))
            .collect();
        let (start, count) = paging(&params);
        let page: Vec<Value> = all
            .iter()
            .skip(start - 1)
            .take(count)
            .map(|u| u.json(base, &self.groups_of(&u.id)))
            .collect();
        Ok(ok(200, list(all.len(), start, page)))
    }

    fn scim_list_groups(&self, base: &str, query: &str) -> R<ScimResponse> {
        let params = query_params(query);
        let filter = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("filter"));
        let conds = filter
            .map(|(_, f)| parse_filter(f))
            .transpose()?
            .unwrap_or_default();
        let members = !excluded_members(query);
        let all: Vec<&ScimGroup> = self
            .scim
            .groups
            .values()
            .filter(|g| conds.iter().all(|c| group_matches(g, c)))
            .collect();
        let (start, count) = paging(&params);
        let page: Vec<Value> = all
            .iter()
            .skip(start - 1)
            .take(count)
            .map(|g| g.json(base, &self.scim.users, members))
            .collect();
        Ok(ok(200, list(all.len(), start, page)))
    }

    fn user_name_taken(&self, name: &str, except: &str) -> bool {
        self.scim
            .users
            .values()
            .any(|u| u.id != except && u.user_name.eq_ignore_ascii_case(name))
    }

    fn save_user(&mut self, u: ScimUser) -> R<()> {
        self.store
            .put_scim_user(&u)
            .map_err(|e| err(500, None, format!("store: {e:#}")))?;
        self.scim.users.insert(u.id.clone(), u);
        Ok(())
    }

    fn save_group(&mut self, g: ScimGroup) -> R<()> {
        self.store
            .put_scim_group(&g)
            .map_err(|e| err(500, None, format!("store: {e:#}")))?;
        self.scim.groups.insert(g.id.clone(), g);
        Ok(())
    }

    fn scim_create_user(&mut self, base: &str, body: &Value, who: &Actor) -> R<ScimResponse> {
        let obj = body
            .as_object()
            .ok_or_else(|| bad("a user is a JSON object"))?;
        if self.scim.users.len() >= MAX_RESOURCES {
            return Err(err(507, None, "too many SCIM users"));
        }
        let now = now_unix();
        let mut u = ScimUser {
            id: uuid::Uuid::new_v4().to_string(),
            created_unix: now,
            updated_unix: now,
            version: 1,
            ..Default::default()
        };
        u.replace_from(obj)?;
        if self.user_name_taken(&u.user_name, "") {
            return Err(err(
                409,
                Some("uniqueness"),
                format!("userName {} exists", u.user_name),
            ));
        }
        self.audit(
            who,
            "scim.user.create",
            &u.user_name,
            format!("active {}", u.active),
            true,
        );
        info!(user = %u.user_name, "SCIM: user provisioned");
        let v = u.json(base, &[]);
        self.save_user(u)?;
        Ok(ok(201, v))
    }

    /// Records a user change, and ends their sessions when they were
    /// turned off.
    fn update_user(
        &mut self,
        base: &str,
        mut u: ScimUser,
        was_active: bool,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        if self.user_name_taken(&u.user_name, &u.id) {
            return Err(err(
                409,
                Some("uniqueness"),
                format!("userName {} exists", u.user_name),
            ));
        }
        u.updated_unix = now_unix();
        u.version += 1;
        let action = match (was_active, u.active) {
            (true, false) => "scim.user.deactivate",
            (false, true) => "scim.user.activate",
            _ => "scim.user.update",
        };
        self.audit(who, action, &u.user_name, String::new(), true);
        if was_active && !u.active {
            fallout
                .end
                .push((u.id.clone(), "turned off in the identity provider"));
        }
        let v = u.json(base, &self.groups_of(&u.id));
        self.save_user(u)?;
        Ok(ok(200, v))
    }

    fn scim_put_user(
        &mut self,
        base: &str,
        id: &str,
        body: &Value,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let obj = body
            .as_object()
            .ok_or_else(|| bad("a user is a JSON object"))?;
        let mut u = self
            .scim
            .users
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("user", id))?;
        let was = u.active;
        u.replace_from(obj)?;
        self.update_user(base, u, was, who, fallout)
    }

    fn scim_patch_user(
        &mut self,
        base: &str,
        id: &str,
        body: &Value,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let mut u = self
            .scim
            .users
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("user", id))?;
        let was = u.active;
        for (op, path, value) in patch_ops(body)? {
            let remove = op == "remove";
            match path {
                Some(p) => u.set(&p.to_ascii_lowercase(), &value, remove)?,
                None => {
                    // No path: the value is an object of attributes (Okta).
                    let obj = value.as_object().ok_or_else(|| {
                        err(
                            400,
                            Some("noTarget"),
                            "a PATCH without a path needs an object",
                        )
                    })?;
                    for (k, v) in obj {
                        if k == ENTERPRISE || k.eq_ignore_ascii_case("schemas") {
                            continue;
                        }
                        u.set(&k.to_ascii_lowercase(), v, remove)?;
                    }
                }
            }
        }
        self.update_user(base, u, was, who, fallout)
    }

    fn scim_delete_user(
        &mut self,
        id: &str,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let u = self
            .scim
            .users
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("user", id))?;
        // Sessions first: ending them needs the person's names.
        self.scim_end_sessions(id, "deleted in the identity provider", who);
        let _ = fallout;
        self.store
            .delete_scim_user(id)
            .map_err(|e| err(500, None, format!("store: {e:#}")))?;
        self.scim.users.remove(id);
        let groups: Vec<ScimGroup> = self
            .scim
            .groups
            .values()
            .filter(|g| g.members.contains(id))
            .cloned()
            .collect();
        for mut g in groups {
            g.members.remove(id);
            self.save_group(g)?;
        }
        self.audit(who, "scim.user.delete", &u.user_name, String::new(), true);
        info!(user = %u.user_name, "SCIM: user deleted");
        Ok(ScimResponse {
            status: 204,
            body: String::new(),
        })
    }

    fn group_from(obj: &Map<String, Value>) -> R<(String, Option<String>, Vec<String>)> {
        let name = get_ci(obj, "displayName")
            .map(opt_str)
            .transpose()?
            .flatten()
            .ok_or_else(|| bad("displayName is required"))?;
        let ext = get_ci(obj, "externalId")
            .map(opt_str)
            .transpose()?
            .flatten();
        let members = get_ci(obj, "members")
            .map(member_ids)
            .transpose()?
            .unwrap_or_default();
        Ok((name, ext, members))
    }

    fn known_members(&self, ids: Vec<String>) -> R<BTreeSet<String>> {
        if ids.len() > MAX_MEMBERS {
            return Err(bad("too many members"));
        }
        // Members we do not know are dropped: a provider may add a group
        // before its people.
        Ok(ids
            .into_iter()
            .filter(|id| self.scim.users.contains_key(id))
            .collect())
    }

    fn scim_create_group(&mut self, base: &str, body: &Value, who: &Actor) -> R<ScimResponse> {
        let obj = body
            .as_object()
            .ok_or_else(|| bad("a group is a JSON object"))?;
        if self.scim.groups.len() >= MAX_RESOURCES {
            return Err(err(507, None, "too many SCIM groups"));
        }
        let (name, ext, members) = Self::group_from(obj)?;
        if self
            .scim
            .groups
            .values()
            .any(|g| g.display_name.eq_ignore_ascii_case(&name))
        {
            return Err(err(409, Some("uniqueness"), format!("group {name} exists")));
        }
        let now = now_unix();
        let g = ScimGroup {
            id: uuid::Uuid::new_v4().to_string(),
            display_name: name,
            external_id: ext,
            members: self.known_members(members)?,
            created_unix: now,
            updated_unix: now,
            version: 1,
        };
        self.audit(
            who,
            "scim.group.create",
            &g.display_name,
            format!("{} member(s)", g.members.len()),
            true,
        );
        let v = g.json(base, &self.scim.users, true);
        self.save_group(g)?;
        Ok(ok(201, v))
    }

    /// Records a group change; members who left a group a rule names
    /// lose their sessions.
    fn update_group(
        &mut self,
        base: &str,
        old: &ScimGroup,
        mut g: ScimGroup,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        g.updated_unix = now_unix();
        g.version += 1;
        let named = |grp: &ScimGroup, maps: &[pumasi_proto::sso::RoleMap]| {
            maps.iter().any(|m| grp.named_by(&m.subject))
        };
        let maps = self.sso.config.maps.clone();
        // Someone who left the group, or everyone when it was renamed
        // away from a rule's name.
        let lost: Vec<String> = if named(old, &maps) && !named(&g, &maps) {
            old.members.iter().cloned().collect()
        } else if named(old, &maps) {
            old.members.difference(&g.members).cloned().collect()
        } else {
            Vec::new()
        };
        for id in lost {
            fallout
                .end
                .push((id, "removed from a group a sign-in rule names"));
        }
        let added = g.members.difference(&old.members).count();
        let removed = old.members.difference(&g.members).count();
        self.audit(
            who,
            "scim.group.update",
            &g.display_name,
            format!("+{added} -{removed} member(s)"),
            true,
        );
        let v = g.json(base, &self.scim.users, true);
        self.save_group(g)?;
        Ok(ok(200, v))
    }

    fn scim_put_group(
        &mut self,
        base: &str,
        id: &str,
        body: &Value,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let obj = body
            .as_object()
            .ok_or_else(|| bad("a group is a JSON object"))?;
        let old = self
            .scim
            .groups
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("group", id))?;
        let (name, ext, members) = Self::group_from(obj)?;
        let mut g = old.clone();
        g.display_name = name;
        g.external_id = ext;
        g.members = self.known_members(members)?;
        self.update_group(base, &old, g, who, fallout)
    }

    fn scim_patch_group(
        &mut self,
        base: &str,
        id: &str,
        body: &Value,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let old = self
            .scim
            .groups
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("group", id))?;
        let mut g = old.clone();
        for (op, path, value) in patch_ops(body)? {
            let lower = path.as_deref().map(str::to_ascii_lowercase);
            match (op.as_str(), lower.as_deref()) {
                ("add", Some("members")) => {
                    let ids = self.known_members(member_ids(&value)?)?;
                    g.members.extend(ids);
                }
                ("replace", Some("members")) => {
                    g.members = self.known_members(member_ids(&value)?)?;
                }
                ("remove", Some("members")) => {
                    let ids = member_ids(&value)?;
                    if ids.is_empty() {
                        g.members.clear();
                    } else {
                        for i in ids {
                            g.members.remove(&i);
                        }
                    }
                }
                ("remove", Some(p)) if p.starts_with("members[") => {
                    // `members[value eq "id"]`.
                    let original = path.as_deref().unwrap_or("");
                    let inner = original
                        .split_once('[')
                        .and_then(|(_, r)| r.rsplit_once(']'))
                        .map(|(f, _)| f)
                        .ok_or_else(|| {
                            err(
                                400,
                                Some("invalidPath"),
                                format!("cannot read {original:?}"),
                            )
                        })?;
                    for c in parse_filter(inner)? {
                        g.members.remove(&c.value);
                    }
                }
                ("add" | "replace", Some("displayname")) => {
                    g.display_name =
                        opt_str(&value)?.ok_or_else(|| bad("displayName is required"))?;
                }
                ("add" | "replace", Some("externalid")) => g.external_id = opt_str(&value)?,
                ("remove", Some("externalid")) => g.external_id = None,
                ("add" | "replace", None) => {
                    let obj = value.as_object().ok_or_else(|| {
                        err(
                            400,
                            Some("noTarget"),
                            "a PATCH without a path needs an object",
                        )
                    })?;
                    if let Some(n) = get_ci(obj, "displayName") {
                        g.display_name =
                            opt_str(n)?.ok_or_else(|| bad("displayName is required"))?;
                    }
                    if let Some(x) = get_ci(obj, "externalId") {
                        g.external_id = opt_str(x)?;
                    }
                    if let Some(m) = get_ci(obj, "members") {
                        let ids = self.known_members(member_ids(m)?)?;
                        if op == "add" {
                            g.members.extend(ids);
                        } else {
                            g.members = ids;
                        }
                    }
                }
                _ => {}
            }
        }
        if g.members.len() > MAX_MEMBERS {
            return Err(bad("too many members"));
        }
        self.update_group(base, &old, g, who, fallout)
    }

    fn scim_delete_group(
        &mut self,
        id: &str,
        who: &Actor,
        fallout: &mut Fallout,
    ) -> R<ScimResponse> {
        let g = self
            .scim
            .groups
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("group", id))?;
        if self.sso.config.maps.iter().any(|m| g.named_by(&m.subject)) {
            for m in &g.members {
                fallout
                    .end
                    .push((m.clone(), "a group a sign-in rule names was deleted"));
            }
        }
        self.store
            .delete_scim_group(id)
            .map_err(|e| err(500, None, format!("store: {e:#}")))?;
        self.scim.groups.remove(id);
        self.audit(
            who,
            "scim.group.delete",
            &g.display_name,
            format!("{} member(s)", g.members.len()),
            true,
        );
        Ok(ScimResponse {
            status: 204,
            body: String::new(),
        })
    }

    /// Revokes every SSO ticket of a provisioned person.
    fn scim_end_sessions(&mut self, user_id: &str, why: &str, who: &Actor) {
        let Some(u) = self.scim.users.get(user_id).cloned() else {
            return;
        };
        let names = u.ticket_names();
        let gone: Vec<crate::manage::AccessRecord> = self
            .mgmt
            .access
            .iter()
            .filter(|r| is_sso_name(&r.name) && names.contains(&r.name))
            .cloned()
            .collect();
        if gone.is_empty() {
            return;
        }
        for r in &gone {
            if let Err(e) = self.store.remove_access(&r.secret) {
                tracing::warn!("revoke {}: {e:#}", r.name);
                continue;
            }
            self.mgmt.access.retain(|x| x.secret != r.secret);
        }
        // Their dashboard sessions end with the tickets they came from.
        let parents: BTreeSet<Secret> = gone.iter().map(|r| r.secret).collect();
        let hashes = self.sessions_of_parents(&parents);
        self.end_sessions(&hashes, why);
        self.audit(
            who,
            "scim.sessions.end",
            &u.user_name,
            format!("{} session(s): {why}", gone.len()),
            true,
        );
        info!(user = %u.user_name, sessions = gone.len(), "SCIM: sessions ended ({why})");
    }
}

fn excluded_members(query: &str) -> bool {
    query_params(query).iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("excludedAttributes")
            && v.split(',')
                .any(|a| a.trim().eq_ignore_ascii_case("members"))
    })
}

fn paging(params: &[(String, String)]) -> (usize, usize) {
    let get = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.parse::<i64>().ok())
    };
    let start = get("startIndex").unwrap_or(1).max(1) as usize;
    let count = get("count").unwrap_or(100).clamp(0, MAX_PAGE as i64) as usize;
    (start, count)
}

fn list(total: usize, start: usize, page: Vec<Value>) -> Value {
    json!({
        "schemas": [LIST_SCHEMA],
        "totalResults": total,
        "startIndex": start,
        "itemsPerPage": page.len(),
        "Resources": page,
    })
}

fn service_provider_config(base: &str) -> Value {
    json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
        "documentationUri": "https://github.com/syang0531/pumasi/blob/main/docs/operations.md#scim-provisioning",
        "patch": {"supported": true},
        "bulk": {"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
        "filter": {"supported": true, "maxResults": MAX_PAGE},
        "changePassword": {"supported": false},
        "sort": {"supported": false},
        "etag": {"supported": false},
        "authenticationSchemes": [{
            "type": "oauthbearertoken",
            "name": "Bearer token",
            "description": "A token from `pumasi sso scim token create`",
            "primary": true,
        }],
        "meta": {"resourceType": "ServiceProviderConfig", "location": format!("{base}/ServiceProviderConfig")},
    })
}

fn resource_types(base: &str) -> Value {
    let t = |name: &str, ep: &str, schema: &str| {
        json!({
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
            "id": name, "name": name, "endpoint": ep, "schema": schema,
            "meta": {"resourceType": "ResourceType", "location": format!("{base}/ResourceTypes/{name}")},
        })
    };
    list(
        2,
        1,
        vec![
            t("User", "/Users", USER_SCHEMA),
            t("Group", "/Groups", GROUP_SCHEMA),
        ],
    )
}

fn schemas() -> Value {
    let attr = |name: &str, kind: &str, multi: bool, required: bool| {
        json!({"name": name, "type": kind, "multiValued": multi, "required": required,
               "caseExact": false, "mutability": "readWrite", "returned": "default", "uniqueness": "none"})
    };
    list(
        2,
        1,
        vec![
            json!({"id": USER_SCHEMA, "name": "User", "attributes": [
                attr("userName", "string", false, true),
                attr("externalId", "string", false, false),
                attr("displayName", "string", false, false),
                attr("name", "complex", false, false),
                attr("emails", "complex", true, false),
                attr("active", "boolean", false, false),
            ]}),
            json!({"id": GROUP_SCHEMA, "name": "Group", "attributes": [
                attr("displayName", "string", false, true),
                attr("externalId", "string", false, false),
                attr("members", "complex", true, false),
            ]}),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters() {
        assert_eq!(
            parse_filter(r#"userName eq "alice@example.com""#).unwrap(),
            vec![Cond {
                attr: "username".into(),
                value: "alice@example.com".into()
            }]
        );
        let c = parse_filter(r#"externalId eq "a\"b" and displayName eq "Ops Team""#).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].value, "a\"b");
        assert_eq!(c[1].value, "Ops Team");
        assert!(parse_filter(r#"userName co "x""#).is_err());
        assert!(parse_filter(r#"userName eq "x" or id eq "y""#).is_err());
        assert!(parse_filter(r#"userName eq "x"#).is_err());
    }

    #[test]
    fn query_strings() {
        let q = query_params("filter=userName%20eq%20%22a%40b.c%22&startIndex=2&count=5");
        assert_eq!(q[0].1, r#"userName eq "a@b.c""#);
        assert_eq!(paging(&q), (2, 5));
        assert_eq!(pct_decode("100%"), "100%");
        assert_eq!(pct_decode("a+b%2"), "a b%2");
    }

    #[test]
    fn entra_patch_values() {
        let mut u = ScimUser {
            user_name: "a".into(),
            active: true,
            ..Default::default()
        };
        u.set("active", &json!("False"), false).unwrap();
        assert!(!u.active);
        u.set(
            r#"emails[type eq "work"].value"#,
            &json!("a@example.com"),
            false,
        )
        .unwrap();
        assert_eq!(u.emails[0].value, "a@example.com");
        assert_eq!(u.emails[0].kind.as_deref(), Some("work"));
        u.set(
            r#"emails[type eq "work"].value"#,
            &json!("b@example.com"),
            false,
        )
        .unwrap();
        assert_eq!(u.emails.len(), 1);
        // Attributes we do not keep are accepted.
        u.set("title", &json!("Engineer"), false).unwrap();
        assert!(u.set("active", &json!("maybe"), false).is_err());
    }

    #[test]
    fn ticket_names_cover_sso_and_ldap() {
        let u = ScimUser {
            user_name: "Alice".into(),
            emails: vec![ScimEmail {
                value: "Alice@Example.com".into(),
                kind: None,
                primary: true,
            }],
            ..Default::default()
        };
        let n = u.ticket_names();
        assert!(n.contains("sso:alice"));
        assert!(n.contains("sso:alice@example.com"));
        assert!(n.contains("ldap:Alice"));
        let who = Identity {
            name: "sso:someone-else".into(),
            verified_email: Some("alice@example.com".into()),
            groups: vec![],
        };
        assert!(u.is(&who));
    }
}

/// The whole path on a real state: provisioning, sessions ending, sign-ins
/// refused, groups that rules name, and the audit log pruned around it.
#[cfg(test)]
mod state_tests {
    use iroh::SecretKey;
    use pumasi_proto::{manage::AccessRole, sso::RoleMap};

    use super::*;
    use crate::store::Store;

    fn open(path: &std::path::Path, key: &SecretKey) -> State {
        State::load(
            "t".into(),
            key.public().to_string(),
            Store::open(path).unwrap(),
            key.clone(),
        )
        .unwrap()
    }

    fn req(token: &str, method: &str, path: &str, body: Value) -> ScimRequest {
        let (path, query) = path.split_once('?').unwrap_or((path, ""));
        ScimRequest {
            token: pumasi_proto::sso::Hidden(token.into()),
            method: method.into(),
            path: path.into(),
            query: query.into(),
            body: if body.is_null() {
                String::new()
            } else {
                body.to_string()
            },
            base_url: "https://ui.example.com/scim/v2".into(),
        }
    }

    fn call(st: &mut State, r: ScimRequest) -> (u16, Value) {
        let resp = st.scim_request(&r, "peer").unwrap_or_else(|e| e);
        let v = if resp.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&resp.body).unwrap()
        };
        (resp.status, v)
    }

    fn who(email: &str, groups: &[&str]) -> Identity {
        Identity {
            name: format!("sso:{email}"),
            verified_email: Some(email.into()),
            groups: groups.iter().map(|g| g.to_string()).collect(),
        }
    }

    #[test]
    fn provisioning_ends_sessions() {
        let path = std::env::temp_dir().join(format!("pumasi-scim-{}.redb", rand::random::<u64>()));
        let key = SecretKey::generate();
        let mut st = open(&path, &key);
        st.sso_update(pumasi_proto::sso::SsoUpdate::Map(RoleMap {
            subject: "group:Pumasi Admins".parse().unwrap(),
            role: AccessRole::Admin,
        }))
        .unwrap();
        st.sso_update(pumasi_proto::sso::SsoUpdate::Map(RoleMap {
            subject: "domain:example.com".parse().unwrap(),
            role: AccessRole::Viewer,
        }))
        .unwrap();
        let (token, _) = st.scim_token_create("Entra ID").unwrap();

        // No token, a wrong one.
        assert_eq!(
            st.scim_request(&req("", "GET", "/Users", Value::Null), "p")
                .unwrap_err()
                .status,
            401
        );
        assert!(
            st.scim_request(&req("pumasi-scim-nope", "GET", "/Users", Value::Null), "p")
                .is_err()
        );

        // Entra ID's first look: is the user there?
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "GET",
                "/Users?filter=userName+eq+%22alice%40example.com%22",
                Value::Null,
            ),
        );
        assert_eq!(s, 200);
        assert_eq!(v["totalResults"], 0);
        // Create, as Entra ID sends it.
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "POST",
                "/Users",
                json!({
                    "schemas": [USER_SCHEMA, ENTERPRISE],
                    "externalId": "0a21f0f2-8d2a",
                    "userName": "alice@example.com",
                    "active": true,
                    "displayName": "Alice",
                    "emails": [{"primary": true, "type": "work", "value": "alice@example.com"}],
                    "name": {"givenName": "Alice", "familyName": "Kim", "formatted": "Alice Kim"},
                    "title": "Engineer",
                    ENTERPRISE: {"department": "Ops"},
                }),
            ),
        );
        assert_eq!(s, 201, "{v}");
        let alice = v["id"].as_str().unwrap().to_string();
        assert_eq!(v["name"]["familyName"], "Kim");
        assert_eq!(
            v["meta"]["location"],
            format!("https://ui.example.com/scim/v2/Users/{alice}")
        );
        // The same user name again: a conflict, without case.
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "POST",
                "/Users",
                json!({"userName": "ALICE@example.com"}),
            ),
        );
        assert_eq!((s, v["scimType"].as_str()), (409, Some("uniqueness")));
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "GET",
                "/Users?filter=userName%20eq%20%22Alice%40Example.com%22",
                Value::Null,
            ),
        );
        assert_eq!((s, v["totalResults"].as_u64()), (200, Some(1)));

        // Bob, by Okta.
        let (_, v) = call(
            &mut st,
            req(
                &token,
                "POST",
                "/Users",
                json!({
                    "userName": "bob@example.com", "active": true,
                    "emails": [{"value": "bob@example.com", "primary": true}],
                }),
            ),
        );
        let bob = v["id"].as_str().unwrap().to_string();

        // A group a rule names, with both.
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "POST",
                "/Groups",
                json!({
                    "displayName": "Pumasi Admins",
                    "members": [{"value": alice}, {"value": bob}, {"value": "not-a-user"}],
                }),
            ),
        );
        assert_eq!(s, 201, "{v}");
        let admins = v["id"].as_str().unwrap().to_string();
        assert_eq!(v["members"].as_array().unwrap().len(), 2);

        // Both sign in.
        let a1 = st
            .sso_session(&who("alice@example.com", &["Pumasi Admins"]), "sso", "p")
            .unwrap();
        let a2 = st
            .sso_session(&who("alice@example.com", &["Pumasi Admins"]), "sso", "p")
            .unwrap();
        st.sso_session(&who("bob@example.com", &["Pumasi Admins"]), "sso", "p")
            .unwrap();
        // A dashboard session of Alice's.
        let (ui, _) = st.start_session(a1.secret, "browser").unwrap();
        assert!(st.access_caller(&a1.secret).is_some());
        assert!(st.find_session(&ui).is_some());

        // Entra ID turns Alice off.
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "PATCH",
                &format!("/Users/{alice}"),
                json!({
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "Replace", "path": "active", "value": "False"}],
                }),
            ),
        );
        assert_eq!(s, 200, "{v}");
        assert_eq!(v["active"], false);
        assert!(
            st.access_caller(&a1.secret).is_none(),
            "her session lives on"
        );
        assert!(st.access_caller(&a2.secret).is_none());
        assert!(
            st.find_session(&ui).is_none(),
            "her dashboard session lives on"
        );
        // Signing in again is refused while she is off.
        let e = st
            .sso_session(&who("alice@example.com", &["Pumasi Admins"]), "sso", "p")
            .unwrap_err();
        assert!(format!("{e:#}").contains("turned off"), "{e:#}");
        // Okta turns her back on (a PATCH without a path).
        let (s, _) = call(
            &mut st,
            req(
                &token,
                "PATCH",
                &format!("/Users/{alice}"),
                json!({
                    "Operations": [{"op": "replace", "value": {"active": true}}],
                }),
            ),
        );
        assert_eq!(s, 200);
        st.sso_session(&who("alice@example.com", &["Pumasi Admins"]), "sso", "p")
            .unwrap();

        // Bob leaves the admins group: his session (an admin one) ends.
        let bob_view = st
            .scim_view()
            .users
            .into_iter()
            .find(|u| u.id == bob)
            .unwrap();
        assert_eq!(bob_view.sessions, 1);
        let (s, v) = call(
            &mut st,
            req(
                &token,
                "PATCH",
                &format!("/Groups/{admins}"),
                json!({
                    "Operations": [{"op": "remove", "path": format!("members[value eq \"{bob}\"]")}],
                }),
            ),
        );
        assert_eq!(s, 200, "{v}");
        let bob_view = st
            .scim_view()
            .users
            .into_iter()
            .find(|u| u.id == bob)
            .unwrap();
        assert_eq!(bob_view.sessions, 0);
        assert!(bob_view.groups.is_empty());
        // He signs in again as what the rules give him now.
        let r = st
            .sso_session(&who("bob@example.com", &[]), "sso", "p")
            .unwrap();
        assert_eq!(r.role, AccessRole::Viewer);

        // Deleting a user ends their sessions and takes them out of groups.
        let (s, _) = call(
            &mut st,
            req(&token, "DELETE", &format!("/Users/{alice}"), Value::Null),
        );
        assert_eq!(s, 204);
        assert_eq!(st.scim_view().users.len(), 1);
        let (s, v) = call(
            &mut st,
            req(&token, "GET", &format!("/Groups/{admins}"), Value::Null),
        );
        assert_eq!(s, 200);
        assert!(v["members"].as_array().unwrap().is_empty());
        let (s, _) = call(
            &mut st,
            req(&token, "GET", &format!("/Users/{alice}"), Value::Null),
        );
        assert_eq!(s, 404);

        // Listing groups without members (Entra ID's check).
        let (_, v) = call(
            &mut st,
            req(
                &token,
                "GET",
                "/Groups?excludedAttributes=members&filter=displayName+eq+%22Pumasi+Admins%22",
                Value::Null,
            ),
        );
        assert_eq!(v["totalResults"], 1);
        assert!(v["Resources"][0].get("members").is_none());

        // Everything survives a restart (and reaches replicas: it is in
        // the state database).
        drop(st);
        let mut st = open(&path, &key);
        assert_eq!(st.scim_view().users.len(), 1);
        assert_eq!(st.scim_view().groups.len(), 1);
        let (s, _) = call(
            &mut st,
            req(&token, "GET", "/ServiceProviderConfig", Value::Null),
        );
        assert_eq!(s, 200);
        // A revoked token stops working.
        let id = st.scim_view().tokens[0].id.clone();
        st.scim_token_revoke(&id).unwrap();
        assert!(
            st.scim_request(&req(&token, "GET", "/Users", Value::Null), "p")
                .is_err()
        );

        // The audit log has all of it, and pruning keeps it provable.
        let head = st.audit_verify().unwrap();
        assert!(head.entries > 10, "{head:?}");
        let through = head.seq / 2;
        let saved = st.audit_from(1, 10_000).unwrap();
        let old_head = pumasi_proto::audit::AuditHead {
            seq: 3,
            hash: saved[2].hash,
            ..head.clone()
        };
        let wrong = st.audit_prune(through, Some([9; 32]), &crate::audit::Actor::system());
        assert!(wrong.is_err());
        let hash = saved[through as usize - 1].hash;
        let (cp, removed) = st
            .audit_prune(through, Some(hash), &crate::audit::Actor::system())
            .unwrap();
        assert_eq!(removed, through);
        cp.verify().unwrap();
        assert!(st.audit_from(1, 10).unwrap()[0].seq == through + 1);
        let after = st.audit_verify().unwrap();
        assert_eq!(after.seq, head.seq + 1, "the prune is an entry of its own");
        assert_eq!(after.entries, after.seq - through);
        // A head saved before the prune checks out against the saved
        // entries and the checkpoint; a changed copy does not.
        let kept: Vec<_> = saved[..through as usize].to_vec();
        pumasi_proto::audit::verify_pruned_head(&old_head, &kept, &cp).unwrap();
        let mut forged = kept.clone();
        forged[1].detail = "changed".into();
        assert!(pumasi_proto::audit::verify_pruned_head(&old_head, &forged, &cp).is_err());
        // The checkpoint survives a restart, and the chain continues.
        drop(st);
        let mut st = open(&path, &key);
        st.audit_verify().unwrap();
        st.audit(&crate::audit::Actor::system(), "test.after", "", "", true);
        let again = st.audit_verify().unwrap();
        assert_eq!(again.seq, after.seq + 1);
        // Nothing before the checkpoint can be pruned twice.
        assert!(
            st.audit_prune(through, None, &crate::audit::Actor::system())
                .is_err()
        );
        // Pruning by age keeps the newest entry.
        assert!(
            st.audit_prune_older(std::time::Duration::from_secs(1), now_unix() + 10)
                .is_some()
        );
        let last = st.audit_verify().unwrap();
        assert!(last.entries >= 2, "{last:?}");
        let _ = std::fs::remove_file(&path);
    }
}
