//! Single sign-on (phase 17): signing in with an organization's OpenID
//! Connect provider (Google Workspace, Microsoft Entra ID, Keycloak, ...)
//! or LDAP directory, and the rules that map its groups to the roles of
//! access tickets.
//!
//! The control plane does all of it: it talks to the identity provider,
//! checks the ID token or the LDAP bind, picks the role and hands out an
//! ordinary access ticket that expires after a session. The dashboard and
//! the CLI only carry the user's browser or terminal there and back.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::manage::AccessRole;

/// A password or client secret: kept out of `Debug` output and logs.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hidden(pub String);

impl fmt::Debug for Hidden {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_empty() { "\"\"" } else { "\"***\"" })
    }
}

impl Hidden {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

/// Sessions last this long unless an admin says otherwise.
pub const DEFAULT_SESSION_HOURS: u32 = 12;
/// The longest session an admin may set (30 days).
pub const MAX_SESSION_HOURS: u32 = 720;

/// An OpenID Connect provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OidcSettings {
    /// Shown on the sign-in button: "Sign in with <name>".
    pub name: String,
    /// The issuer URL; `<issuer>/.well-known/openid-configuration` must
    /// describe it. HTTPS, except on loopback (for tests).
    pub issuer: String,
    pub client_id: String,
    /// Kept on the control plane only; never listed.
    pub client_secret: Hidden,
    /// Scopes asked for besides `openid` (default `email profile`).
    pub scopes: Vec<String>,
    /// The ID token claim with the user's groups (default `groups`).
    pub groups_claim: String,
}

/// An LDAP directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LdapSettings {
    /// `ldaps://host:636` or `ldap://host:389` (with `starttls`). Plain
    /// `ldap://` without StartTLS only on loopback.
    pub url: String,
    pub starttls: bool,
    /// The account that looks users up; empty for an anonymous search.
    pub bind_dn: String,
    /// Kept on the control plane only; never listed.
    pub bind_password: Hidden,
    /// Where users are, e.g. `ou=people,dc=example,dc=com`.
    pub user_base: String,
    /// `{user}` is replaced by the escaped user name, e.g. `(uid={user})`
    /// or `(sAMAccountName={user})`.
    pub user_filter: String,
    /// The user attribute that lists group DNs (default `memberOf`).
    pub group_attr: String,
    /// Also search groups here (for directories without `memberOf`).
    pub group_base: String,
    /// `{dn}` and `{user}` are replaced, e.g. `(member={dn})`.
    pub group_filter: String,
}

/// Who a sign-in rule matches.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Subject {
    /// A group from the ID token's groups claim, or an LDAP group (its
    /// whole DN or its `cn`). Compared without case.
    Group(String),
    /// One verified email address.
    Email(String),
    /// Every verified email address of a domain.
    Domain(String),
    /// Anyone the provider or directory accepts.
    Anyone,
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::Group(g) => write!(f, "group:{g}"),
            Subject::Email(e) => write!(f, "email:{e}"),
            Subject::Domain(d) => write!(f, "domain:{d}"),
            Subject::Anyone => f.write_str("anyone"),
        }
    }
}

impl FromStr for Subject {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        if s == "anyone" || s == "*" {
            return Ok(Subject::Anyone);
        }
        let (kind, value) = s
            .split_once(':')
            .context("a rule is group:NAME, email:ADDRESS, domain:DOMAIN or anyone")?;
        let value = value.trim();
        ensure!(!value.is_empty(), "the rule {s:?} names nothing");
        ensure!(value.len() <= 512, "the rule is too long");
        ensure!(
            !value.chars().any(char::is_control),
            "the rule has control characters"
        );
        Ok(match kind.trim() {
            "group" => Subject::Group(value.to_string()),
            "email" => {
                ensure!(value.contains('@'), "{value:?} is not an email address");
                Subject::Email(value.to_ascii_lowercase())
            }
            "domain" => {
                let d = value.trim_start_matches('@').to_ascii_lowercase();
                ensure!(
                    d.contains('.') && !d.contains('@'),
                    "{value:?} is not a domain"
                );
                Subject::Domain(d)
            }
            other => bail!("unknown rule kind {other:?}: use group:, email:, domain: or anyone"),
        })
    }
}

/// Who signed in, as the provider or directory said.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    /// `sso:<email or subject>` or `ldap:<user>`: the access ticket's name.
    pub name: String,
    /// Lowercase, only when the provider verified it.
    pub verified_email: Option<String>,
    pub groups: Vec<String>,
}

impl Subject {
    pub fn matches(&self, who: &Identity) -> bool {
        match self {
            Subject::Anyone => true,
            Subject::Group(g) => who.groups.iter().any(|have| group_matches(g, have)),
            Subject::Email(e) => who.verified_email.as_deref() == Some(e.as_str()),
            Subject::Domain(d) => who
                .verified_email
                .as_deref()
                .and_then(|e| e.rsplit_once('@'))
                .is_some_and(|(_, domain)| domain == d),
        }
    }
}

/// `want` is a group name or DN; `have` is what the provider listed. An
/// LDAP DN also matches by its first `cn`.
fn group_matches(want: &str, have: &str) -> bool {
    if want.eq_ignore_ascii_case(have) {
        return true;
    }
    let first = have.split(',').next().unwrap_or("");
    match first.split_once('=') {
        Some((k, v)) if k.trim().eq_ignore_ascii_case("cn") => v.trim().eq_ignore_ascii_case(want),
        _ => false,
    }
}

/// One rule: whoever `subject` matches gets `role`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleMap {
    pub subject: Subject,
    pub role: AccessRole,
}

/// The role for `who`: the most powerful one any rule gives (admin, then
/// submitter, then viewer); among submitter rules the first listed. `None`
/// when no rule matches: the sign-in is refused.
pub fn role_for(maps: &[RoleMap], who: &Identity) -> Option<AccessRole> {
    let rank = |r: &AccessRole| match r {
        AccessRole::Admin => 0,
        AccessRole::Submitter { .. } => 1,
        AccessRole::Viewer => 2,
    };
    maps.iter()
        .filter(|m| m.subject.matches(who))
        .min_by_key(|m| rank(&m.role))
        .map(|m| m.role.clone())
}

/// A change to the SSO settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SsoUpdate {
    /// Set or (with `None`) turn off the OpenID Connect provider.
    Oidc(Option<Box<OidcSettings>>),
    /// Set or turn off the LDAP directory.
    Ldap(Option<Box<LdapSettings>>),
    /// Add a rule, or replace the role of the rule with the same subject.
    Map(RoleMap),
    /// Remove the rule for a subject.
    Unmap(Subject),
    /// How long a sign-in lasts.
    SessionHours(u32),
}

/// The settings as an admin sees them: without the client secret or the
/// bind password.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsoView {
    pub oidc: Option<OidcView>,
    pub ldap: Option<LdapView>,
    pub maps: Vec<RoleMap>,
    pub session_hours: u32,
    /// Live SSO sessions (access tickets named `sso:` or `ldap:`).
    pub sessions: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OidcView {
    pub name: String,
    pub issuer: String,
    pub client_id: String,
    pub scopes: Vec<String>,
    pub groups_claim: String,
    pub has_secret: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LdapView {
    pub url: String,
    pub starttls: bool,
    pub bind_dn: String,
    pub user_base: String,
    pub user_filter: String,
    pub group_attr: String,
    pub group_base: String,
    pub group_filter: String,
}

/// What the sign-in page and `pumasi login` may offer, for anyone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsoInfo {
    /// The OpenID Connect provider's name, if there is one.
    pub oidc: Option<String>,
    /// Whether it offers the device flow (for `pumasi login --sso`).
    pub device: bool,
    pub ldap: bool,
}

/// A device sign-in under way: the user opens `verification_uri` and
/// types `user_code`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceStart {
    /// Names this sign-in in `SsoDevicePoll`.
    pub handle: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    pub interval_secs: u64,
    pub expires_secs: u64,
}

impl OidcSettings {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.name.trim().is_empty(), "the provider needs a name");
        ensure!(!self.client_id.is_empty(), "--client-id is empty");
        ensure!(
            !self.groups_claim.is_empty(),
            "the groups claim cannot be empty"
        );
        check_url(&self.issuer, &["https"], &["http"]).context("issuer")?;
        for s in &self.scopes {
            ensure!(
                !s.is_empty() && s.chars().all(|c| c.is_ascii_graphic() && c != '"'),
                "bad scope {s:?}"
            );
        }
        Ok(())
    }

    pub fn view(&self) -> OidcView {
        OidcView {
            name: self.name.clone(),
            issuer: self.issuer.clone(),
            client_id: self.client_id.clone(),
            scopes: self.scopes.clone(),
            groups_claim: self.groups_claim.clone(),
            has_secret: !self.client_secret.is_empty(),
        }
    }
}

impl LdapSettings {
    pub fn validate(&self) -> Result<()> {
        // StartTLS turns ldap:// into TLS before anything is sent.
        let secure: &[&str] = if self.starttls {
            &["ldaps", "ldap"]
        } else {
            &["ldaps"]
        };
        check_url(&self.url, secure, &["ldap"]).context("LDAP URL")?;
        ensure!(!self.user_base.is_empty(), "--user-base is empty");
        ensure!(
            self.user_filter.contains("{user}"),
            "the user filter must contain {{user}}"
        );
        ensure!(
            self.group_base.is_empty() || self.group_filter.contains("{dn}") || self.group_filter.contains("{user}"),
            "the group filter must contain {{dn}} or {{user}}"
        );
        Ok(())
    }

    pub fn view(&self) -> LdapView {
        LdapView {
            url: self.url.clone(),
            starttls: self.starttls,
            bind_dn: self.bind_dn.clone(),
            user_base: self.user_base.clone(),
            user_filter: self.user_filter.clone(),
            group_attr: self.group_attr.clone(),
            group_base: self.group_base.clone(),
            group_filter: self.group_filter.clone(),
        }
    }
}

/// `url` must use one of `secure` schemes, or one of `loopback_only` on a
/// loopback host.
pub fn check_url(url: &str, secure: &[&str], loopback_only: &[&str]) -> Result<()> {
    let (scheme, rest) = url
        .split_once("://")
        .with_context(|| format!("{url:?} is not a URL"))?;
    let scheme = scheme.to_ascii_lowercase();
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    ensure!(!host.is_empty(), "{url:?} has no host");
    if secure.contains(&scheme.as_str()) {
        return Ok(());
    }
    if loopback_only.contains(&scheme.as_str()) && is_loopback_host(host) {
        return Ok(());
    }
    bail!(
        "{url:?}: use {} (plain {scheme} only to this machine)",
        secure.join(" or ")
    )
}

fn is_loopback_host(host_port: &str) -> bool {
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host_port.rsplit_once(':').map_or(host_port, |(h, _)| h)
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn who(email: Option<&str>, groups: &[&str]) -> Identity {
        Identity {
            name: "sso:x".into(),
            verified_email: email.map(str::to_string),
            groups: groups.iter().map(|g| g.to_string()).collect(),
        }
    }

    #[test]
    fn subjects_parse_and_match() {
        let g: Subject = "group:Engineering".parse().unwrap();
        assert!(g.matches(&who(None, &["engineering"])));
        assert!(g.matches(&who(None, &["cn=engineering,ou=groups,dc=x"])));
        assert!(!g.matches(&who(None, &["ou=engineering,dc=x"])));
        let dn: Subject = "group:cn=ops,ou=groups,dc=x".parse().unwrap();
        assert!(dn.matches(&who(None, &["CN=ops,OU=groups,DC=x"])));
        let e: Subject = "email:Alice@Example.com".parse().unwrap();
        assert_eq!(e.to_string(), "email:alice@example.com");
        assert!(e.matches(&who(Some("alice@example.com"), &[])));
        assert!(!e.matches(&who(None, &[])), "unverified emails never match");
        let d: Subject = "domain:@example.com".parse().unwrap();
        assert!(d.matches(&who(Some("bob@example.com"), &[])));
        assert!(!d.matches(&who(Some("bob@example.com.evil.io"), &[])));
        assert!(!d.matches(&who(Some("bob@sub.example.com"), &[])));
        assert!("anyone".parse::<Subject>().unwrap().matches(&who(None, &[])));
        for bad in ["", "group:", "foo:bar", "email:nope", "domain:x", "group:a\nb"] {
            assert!(bad.parse::<Subject>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_most_powerful_role_wins() {
        let maps = vec![
            RoleMap {
                subject: Subject::Anyone,
                role: AccessRole::Viewer,
            },
            RoleMap {
                subject: Subject::Group("ml".into()),
                role: AccessRole::Submitter {
                    account: "ml".into(),
                },
            },
            RoleMap {
                subject: Subject::Group("web".into()),
                role: AccessRole::Submitter {
                    account: "web".into(),
                },
            },
            RoleMap {
                subject: Subject::Email("root@x.io".into()),
                role: AccessRole::Admin,
            },
        ];
        assert_eq!(role_for(&maps, &who(None, &[])), Some(AccessRole::Viewer));
        assert_eq!(
            role_for(&maps, &who(None, &["web", "ml"])),
            Some(AccessRole::Submitter {
                account: "ml".into()
            })
        );
        assert_eq!(
            role_for(&maps, &who(Some("root@x.io"), &["ml"])),
            Some(AccessRole::Admin)
        );
        assert_eq!(role_for(&maps[1..2], &who(None, &["web"])), None);
    }

    #[test]
    fn secrets_stay_out_of_debug() {
        let h = Hidden("hunter2".into());
        assert_eq!(format!("{h:?}"), "\"***\"");
        assert_eq!(serde_json::to_string(&h).unwrap(), "\"hunter2\"");
        let op = crate::msg::AdminOp::LdapLogin {
            user: "bob".into(),
            password: h,
        };
        assert!(!format!("{op:?}").contains("hunter2"));
    }

    #[test]
    fn urls() {
        check_url("https://idp.example.com/realms/x", &["https"], &["http"]).unwrap();
        check_url("http://127.0.0.1:8080/x", &["https"], &["http"]).unwrap();
        check_url("http://localhost:8080", &["https"], &["http"]).unwrap();
        check_url("http://[::1]:8080", &["https"], &["http"]).unwrap();
        assert!(check_url("http://idp.example.com", &["https"], &["http"]).is_err());
        assert!(check_url("http://127.0.0.1.evil.io", &["https"], &["http"]).is_err());
        assert!(check_url("http://user@127.0.0.1@evil.io", &["https"], &["http"]).is_err());
        assert!(check_url("ftp://x", &["https"], &["http"]).is_err());
        assert!(check_url("https://", &["https"], &["http"]).is_err());
        let mut l = LdapSettings {
            url: "ldap://dir.example.com".into(),
            starttls: false,
            bind_dn: String::new(),
            bind_password: Hidden::default(),
            user_base: "ou=people,dc=x".into(),
            user_filter: "(uid={user})".into(),
            group_attr: "memberOf".into(),
            group_base: String::new(),
            group_filter: String::new(),
        };
        assert!(l.validate().is_err());
        l.starttls = true;
        l.validate().unwrap();
        l.url = "ldaps://dir.example.com".into();
        l.starttls = false;
        l.validate().unwrap();
        l.user_filter = "(uid=bob)".into();
        assert!(l.validate().is_err());
    }
}
