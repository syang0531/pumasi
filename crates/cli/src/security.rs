//! Self-hosting 1.0 commands (phase 17): the audit log (`pumasi audit`),
//! SSO settings (`pumasi sso`), signing in with SSO or LDAP (`pumasi login
//! --sso`, `--ldap`) and replacing the admin ticket (`pumasi admin
//! rotate`).

use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use pumasi_cluster::{ADMIN_TICKET_FILE, AdminClient};
use pumasi_proto::{
    audit::{AuditEntry, AuditHead, syslog_line},
    manage::AccessRole,
    msg::{AdminOp, AdminResponse},
    sso::{Hidden, LdapSettings, OidcSettings, RoleMap, SsoUpdate, SsoView, Subject},
    ticket::SignInTicket,
};

use crate::{AdminArgs, connect_admin, load_ticket, net_for_all, utc};

#[derive(Subcommand)]
pub enum AuditCmd {
    /// Show audit entries, oldest first (the newest 50 unless --after).
    Ls {
        /// Entries after this number.
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// One JSON object per line (with the hashes).
        #[arg(long)]
        json: bool,
        /// Keep printing new entries as they come.
        #[arg(short, long)]
        follow: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Check the whole hash chain and print its head, signed by the
    /// cluster key. Keep the head somewhere else: a later check that does
    /// not reach it means the log was rewritten.
    Verify {
        /// Print the signed head as JSON.
        #[arg(long)]
        json: bool,
        /// A head saved earlier (JSON): check that the log still contains
        /// it.
        #[arg(long)]
        against: Option<PathBuf>,
        /// The entries a prune removed (the file `pumasi audit prune
        /// --save` wrote), for a saved head older than the prune.
        #[arg(long)]
        pruned: Option<PathBuf>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Remove old entries behind a checkpoint signed by the cluster key
    /// (1.12). The whole chain is checked first; afterwards `verify`
    /// checks the rest from the checkpoint. Without --yes, only says what
    /// would go.
    Prune {
        /// Remove entries older than this: a duration (365d) or a UTC date
        /// (2026-01-01).
        #[arg(long, conflicts_with = "through")]
        before: Option<String>,
        /// Remove entries up to and including this number.
        #[arg(long)]
        through: Option<u64>,
        /// Save the entries first, one JSON object per line, with the
        /// checkpoint next to them (FILE.checkpoint.json).
        #[arg(long, value_name = "FILE")]
        save: Option<PathBuf>,
        /// Do not save them (they are gone for good unless exported).
        #[arg(long, conflicts_with = "save")]
        no_save: bool,
        /// Prune now.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Send entries to a syslog server (RFC 5424, facility authpriv), or
    /// print them as syslog lines. With --follow it keeps sending; with
    /// --state it resumes where it stopped.
    Export {
        /// `udp://host:514` or `tcp://host:601` (octet-counted frames).
        /// Without it, lines go to standard output.
        #[arg(long)]
        syslog: Option<String>,
        /// A file that remembers the last entry sent.
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(short, long)]
        follow: bool,
        /// The host name in each message (default: the cluster's name).
        #[arg(long)]
        hostname: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Subcommand)]
pub enum SsoCmd {
    /// Show the SSO settings and rules (no secrets).
    Status {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Sign in with an OpenID Connect provider: Google Workspace,
    /// Microsoft Entra ID, Keycloak, Okta, ... Register a confidential
    /// web client there with the dashboard's `/sso/callback` as redirect
    /// URI (and the device flow allowed, for `pumasi login --sso`).
    Oidc {
        /// The issuer URL, e.g. https://accounts.google.com,
        /// https://login.microsoftonline.com/<tenant>/v2.0 or
        /// https://keycloak.example.com/realms/<realm>.
        #[arg(long, required_unless_present = "off")]
        issuer: Option<String>,
        #[arg(long, required_unless_present = "off")]
        client_id: Option<String>,
        /// A file with the client secret (not a flag: flags end up in
        /// shell history).
        #[arg(long, conflicts_with = "client_secret_env")]
        client_secret_file: Option<PathBuf>,
        /// An environment variable with the client secret.
        #[arg(long)]
        client_secret_env: Option<String>,
        /// The name on the sign-in button.
        #[arg(long, default_value = "SSO")]
        name: String,
        /// Scopes besides openid.
        #[arg(long, value_delimiter = ',', default_value = "email,profile")]
        scopes: Vec<String>,
        /// The ID token claim with the groups.
        #[arg(long, default_value = "groups")]
        groups_claim: String,
        /// Turn OpenID Connect sign-in off.
        #[arg(long)]
        off: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Sign in with an LDAP directory (Active Directory, OpenLDAP, ...).
    Ldap {
        /// ldaps://host:636, or ldap://host:389 with --starttls.
        #[arg(long, required_unless_present = "off")]
        url: Option<String>,
        #[arg(long)]
        starttls: bool,
        /// The service account that looks users up (default: anonymous).
        #[arg(long, default_value = "")]
        bind_dn: String,
        /// A file with the service account's password.
        #[arg(long)]
        bind_password_file: Option<PathBuf>,
        /// Where users are, e.g. ou=people,dc=example,dc=com.
        #[arg(long, required_unless_present = "off")]
        user_base: Option<String>,
        /// `{user}` is the escaped user name; Active Directory:
        /// (sAMAccountName={user}).
        #[arg(long, default_value = "(uid={user})")]
        user_filter: String,
        #[arg(long, default_value = "memberOf")]
        group_attr: String,
        /// Also search groups here (directories without memberOf).
        #[arg(long, default_value = "")]
        group_base: String,
        /// `{dn}` and `{user}` are replaced, e.g. (member={dn}).
        #[arg(long, default_value = "(member={dn})")]
        group_filter: String,
        /// Turn LDAP sign-in off.
        #[arg(long)]
        off: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Give whoever matches RULE a role: `group:NAME` (an ID token group,
    /// or an LDAP group's DN, or its cn if it is under --group-base), `email:ADDRESS`, `domain:DOMAIN`
    /// (verified emails only) or `anyone`. Someone several rules match
    /// gets the most powerful role.
    Map {
        rule: String,
        /// admin, viewer, or submitter:ACCOUNT.
        role: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Remove the rule for RULE.
    Unmap {
        rule: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Allow or refuse sign-in from a terminal (`pumasi login --sso`, the
    /// OpenID Connect device flow). Off by default: anyone can start a
    /// device sign-in and talk a member into entering its code at the
    /// provider, and the session is then theirs, with the member's role.
    Device {
        #[arg(value_parser = ["on", "off"])]
        state: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// How long a sign-in lasts, e.g. 12h or 7d (at most 30d).
    Session {
        length: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Print a sign-in ticket for the people of the organization: the
    /// cluster's address with no secret. They run `pumasi login --sso
    /// <ticket>`.
    Ticket {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// SCIM provisioning (1.12): tokens for identity providers, and the
    /// people and groups they pushed.
    #[command(subcommand)]
    Scim(ScimCmd),
}

#[derive(Subcommand)]
pub enum ScimCmd {
    /// The SCIM tokens, people and groups.
    Status {
        /// As JSON.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// A new token for an identity provider (printed once). The provider
    /// calls `<dashboard URL>/scim/v2` with it as a bearer token.
    Token {
        /// The provider's name, e.g. "Entra ID".
        label: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Revoke a token (its id, a unique prefix, or its label).
    Revoke {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Subcommand)]
pub enum AdminCmd {
    /// Replace the cluster's admin secret: every copy of the current admin
    /// ticket stops working (access tickets and SSO sessions keep
    /// working). The new ticket is saved here and printed once.
    Rotate {
        #[command(flatten)]
        admin: AdminArgs,
    },
}

async fn call(home: &Path, admin: &AdminArgs, op: AdminOp) -> Result<AdminResponse> {
    let client = connect_admin(home, admin).await?;
    let r = client.call(op).await;
    client.close().await;
    r
}

fn print_entries(entries: &[AuditEntry], json: bool) -> Result<()> {
    let mut out = std::io::stdout().lock();
    for e in entries {
        if json {
            writeln!(out, "{}", serde_json::to_string(e)?)?;
        } else {
            writeln!(out, "{}", e.line())?;
        }
    }
    out.flush()?;
    Ok(())
}

async fn fetch(client: &AdminClient, after: u64, limit: u32) -> Result<Vec<AuditEntry>> {
    match client.call(AdminOp::Audit { after, limit }).await? {
        AdminResponse::Audit(v) => Ok(v),
        _ => bail!("unexpected response"),
    }
}

pub async fn audit(home: &Path, cmd: AuditCmd) -> Result<i32> {
    match cmd {
        AuditCmd::Ls {
            after,
            limit,
            json,
            follow,
            admin,
        } => {
            let client = connect_admin(home, &admin).await?;
            let mut last = after;
            loop {
                let v = fetch(&client, last, limit).await?;
                if let Some(e) = v.last() {
                    last = e.seq;
                }
                print_entries(&v, json)?;
                if !follow {
                    break;
                }
                if v.len() < limit as usize {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                        _ = crate::ctrl_c() => break,
                    }
                }
            }
            client.close().await;
            Ok(0)
        }
        AuditCmd::Verify {
            json,
            against,
            pruned,
            admin,
        } => {
            let client = connect_admin(home, &admin).await?;
            let head = match client.call(AdminOp::AuditVerify).await? {
                AdminResponse::AuditHead(h) => *h,
                _ => bail!("unexpected response"),
            };
            head.verify()?;
            anyhow::ensure!(
                head.cluster == client.cluster_id().to_string(),
                "the head is signed by {}, not this cluster's key",
                head.cluster
            );
            let checkpoint = audit_checkpoint(&client).await?;
            if let Some(cp) = &checkpoint {
                cp.verify().context("the audit checkpoint")?;
                anyhow::ensure!(
                    cp.cluster == head.cluster,
                    "the audit checkpoint is signed by {}, not this cluster's key",
                    cp.cluster
                );
                eprintln!("{}", cp.line());
            }
            if let Some(path) = against {
                let old: AuditHead = serde_json::from_str(
                    &std::fs::read_to_string(&path)
                        .with_context(|| format!("read {}", path.display()))?,
                )?;
                old.verify().context("the saved head")?;
                anyhow::ensure!(
                    old.cluster == head.cluster,
                    "the saved head is of another cluster"
                );
                anyhow::ensure!(
                    old.seq <= head.seq,
                    "the log ends at #{} but the saved head is #{}: entries were removed",
                    head.seq,
                    old.seq
                );
                let floor = checkpoint.as_ref().map_or(0, |c| c.seq);
                if old.seq > 0 && old.seq <= floor {
                    let cp = checkpoint.as_ref().expect("floor > 0");
                    if old.seq == cp.seq {
                        anyhow::ensure!(
                            old.hash == cp.hash,
                            "the checkpoint at #{} does not have the saved head's hash: the log was rewritten",
                            cp.seq
                        );
                    } else {
                        let Some(file) = &pruned else {
                            bail!(
                                "the saved head #{} was pruned (the log starts at #{}); give the entries the prune saved with --pruned FILE",
                                old.seq,
                                floor + 1
                            );
                        };
                        let saved = read_entries(file)?;
                        pumasi_proto::audit::verify_pruned_head(&old, &saved, cp)?;
                    }
                } else if old.seq > 0 {
                    let e = fetch(&client, old.seq - 1, 1).await?;
                    anyhow::ensure!(
                        e.first()
                            .is_some_and(|e| e.seq == old.seq && e.hash == old.hash),
                        "entry #{} no longer has the saved hash: the log was rewritten",
                        old.seq
                    );
                }
                eprintln!("the log still holds the saved head #{}", old.seq);
            }
            client.close().await;
            if json {
                println!("{}", serde_json::to_string_pretty(&head)?);
            } else {
                println!(
                    "audit log intact: {} entries, head #{} {} (signed by cluster {}, {} UTC)",
                    head.entries,
                    head.seq,
                    data_encoding::HEXLOWER.encode(&head.hash),
                    &head.cluster[..10.min(head.cluster.len())],
                    utc(head.unix)
                );
            }
            Ok(0)
        }
        AuditCmd::Export {
            syslog,
            state,
            after,
            follow,
            hostname,
            admin,
        } => export(home, syslog, state, after, follow, hostname, admin).await,
        AuditCmd::Prune {
            before,
            through,
            save,
            no_save,
            yes,
            admin,
        } => prune(home, before, through, save, no_save, yes, admin).await,
    }
}

/// The latest prune's checkpoint; `None` from a control plane older than
/// 1.12, which never prunes.
async fn audit_checkpoint(
    client: &AdminClient,
) -> Result<Option<pumasi_proto::audit::AuditCheckpoint>> {
    match client.call(AdminOp::AuditCheckpoint).await {
        Ok(AdminResponse::AuditCheckpoint(c)) => Ok(c.map(|b| *b)),
        Ok(AdminResponse::Error(e)) if e.contains("unexpected admin call") => Ok(None),
        Ok(AdminResponse::Error(e)) => bail!("{e}"),
        Ok(_) => bail!("unexpected response"),
        // An older control plane cannot read the call and drops it.
        Err(_) => Ok(None),
    }
}

/// Audit entries as `--json` and `prune --save` write them.
fn read_entries(path: &Path) -> Result<Vec<AuditEntry>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l)
                .with_context(|| format!("{} line {}: not an audit entry", path.display(), i + 1))
        })
        .collect()
}

/// The UNIX time `before` names: a duration ago, or a UTC date.
fn cutoff(before: &str, now: u64) -> Result<u64> {
    let b = before.trim();
    if let Ok(d) = chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d") {
        let t = d
            .and_hms_opt(0, 0, 0)
            .context("bad date")?
            .and_utc()
            .timestamp();
        return u64::try_from(t).context("a date before 1970");
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(b) {
        return u64::try_from(t.timestamp()).context("a date before 1970");
    }
    let ms = pumasi_proto::job::parse_duration_ms(b)
        .with_context(|| format!("--before {b:?}: a duration (365d) or a date (2026-01-01)"))?;
    Ok(now.saturating_sub(ms / 1000))
}

async fn prune(
    home: &Path,
    before: Option<String>,
    through: Option<u64>,
    save: Option<PathBuf>,
    no_save: bool,
    yes: bool,
    admin: AdminArgs,
) -> Result<i32> {
    anyhow::ensure!(
        before.is_some() || through.is_some(),
        "say what to prune: --before 365d (or a date) or --through NUMBER"
    );
    anyhow::ensure!(
        save.is_some() || no_save,
        "give --save FILE to keep the entries (or --no-save to drop them)"
    );
    if let Some(path) = &save {
        anyhow::ensure!(!path.exists(), "{} exists; give a new file", path.display());
    }
    let client = connect_admin(home, &admin).await?;
    let floor = audit_checkpoint(&client).await?.map_or(0, |c| c.seq);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let cut = before.as_deref().map(|b| cutoff(b, now)).transpose()?;
    // Read what would go, oldest first.
    let mut doomed: Vec<AuditEntry> = Vec::new();
    let mut from = floor + 1;
    let mut newest = floor;
    'pages: loop {
        let page = match client
            .call(AdminOp::AuditFrom { from, limit: 5000 })
            .await?
        {
            AdminResponse::Audit(v) => v,
            AdminResponse::Error(e) => bail!("{e}"),
            _ => bail!("unexpected response"),
        };
        let Some(last) = page.last() else { break };
        from = last.seq + 1;
        for e in &page {
            newest = newest.max(e.seq);
            let goes = match (through, cut) {
                (Some(t), _) => e.seq <= t,
                (None, Some(c)) => e.unix < c,
                _ => false,
            };
            if !goes {
                break 'pages;
            }
            doomed.push(e.clone());
        }
        if page.len() < 5000 {
            break;
        }
    }
    // Keep the newest entry: the head stays where `verify` finds it.
    while doomed.last().is_some_and(|e| e.seq >= newest) && through.is_none() {
        doomed.pop();
    }
    let Some(last) = doomed.last().cloned() else {
        eprintln!("nothing to prune");
        client.close().await;
        return Ok(0);
    };
    if let Some(t) = through {
        anyhow::ensure!(
            last.seq == t,
            "the log has no entry #{t} to prune up to (it ends at #{})",
            last.seq
        );
    }
    let first = doomed.first().map_or(0, |e| e.seq);
    eprintln!(
        "{} entries (#{first} to #{}, up to {} UTC) would be pruned",
        doomed.len(),
        last.seq,
        pumasi_proto::audit::rfc3339(last.unix)
    );
    if !yes {
        eprintln!("run again with --yes to prune");
        client.close().await;
        return Ok(1);
    }
    if let Some(path) = &save {
        let mut out = String::new();
        for e in &doomed {
            out.push_str(&serde_json::to_string(e)?);
            out.push('\n');
        }
        std::fs::write(path, out).with_context(|| format!("write {}", path.display()))?;
        eprintln!("saved them to {}", path.display());
    }
    let cp = match client
        .call(AdminOp::AuditPrune {
            through: last.seq,
            hash: last.hash,
        })
        .await?
    {
        AdminResponse::AuditCheckpoint(Some(c)) => *c,
        AdminResponse::Error(e) => bail!("{e}"),
        _ => bail!("unexpected response (is the control plane older than 1.12?)"),
    };
    cp.verify().context("the new checkpoint")?;
    if let Some(path) = &save {
        let side = PathBuf::from(format!("{}.checkpoint.json", path.display()));
        std::fs::write(&side, serde_json::to_string_pretty(&cp)? + "\n")
            .with_context(|| format!("write {}", side.display()))?;
    }
    client.close().await;
    println!("{}", cp.line());
    Ok(0)
}

enum Sink {
    Stdout,
    Udp(tokio::net::UdpSocket),
    Tcp(tokio::net::TcpStream),
}

async fn open_sink(url: Option<&str>) -> Result<Sink> {
    let Some(url) = url else {
        return Ok(Sink::Stdout);
    };
    if let Some(addr) = url.strip_prefix("udp://") {
        let sock = tokio::net::UdpSocket::bind(if addr.starts_with('[') {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        })
        .await?;
        sock.connect(addr)
            .await
            .with_context(|| format!("syslog server {addr}"))?;
        return Ok(Sink::Udp(sock));
    }
    if let Some(addr) = url.strip_prefix("tcp://") {
        let s = tokio::net::TcpStream::connect(addr)
            .await
            .with_context(|| format!("syslog server {addr}"))?;
        return Ok(Sink::Tcp(s));
    }
    bail!("--syslog takes udp://host:port or tcp://host:port")
}

impl Sink {
    async fn send(&mut self, line: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            Sink::Stdout => {
                println!("{line}");
                Ok(())
            }
            Sink::Udp(s) => {
                s.send(line.as_bytes()).await?;
                Ok(())
            }
            // RFC 6587 octet counting.
            Sink::Tcp(s) => {
                s.write_all(format!("{} {line}", line.len()).as_bytes())
                    .await?;
                Ok(())
            }
        }
    }
}

async fn export(
    home: &Path,
    syslog: Option<String>,
    state: Option<PathBuf>,
    after: u64,
    follow: bool,
    hostname: Option<String>,
    admin: AdminArgs,
) -> Result<i32> {
    let client = connect_admin(home, &admin).await?;
    let host = match hostname {
        Some(h) => h,
        None => match client.call(AdminOp::Status).await? {
            AdminResponse::Status(s) => s.cluster_name,
            _ => "pumasi".into(),
        },
    };
    let mut last = after;
    if let Some(p) = &state
        && let Ok(s) = std::fs::read_to_string(p)
    {
        last = last.max(
            s.trim()
                .parse()
                .context("the --state file holds no number")?,
        );
    }
    let mut sink = open_sink(syslog.as_deref()).await?;
    let mut sent = 0u64;
    loop {
        let v = fetch(&client, last, 500).await?;
        if last > 0
            && let Some(first) = v.first()
            && first.seq > last + 1
        {
            eprintln!(
                "warning: entries #{} to #{} were pruned before they were sent; the export goes on from #{}",
                last + 1,
                first.seq - 1,
                first.seq
            );
        }
        for e in &v {
            sink.send(&syslog_line(e, &host)).await?;
            last = e.seq;
            sent += 1;
        }
        if let (Some(p), false) = (&state, v.is_empty()) {
            pumasi_proto::net::write_private(p, last.to_string().as_bytes())?;
        }
        if v.len() == 500 {
            continue;
        }
        if !follow {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            _ = crate::ctrl_c() => break,
        }
    }
    client.close().await;
    if syslog.is_some() {
        eprintln!("sent {sent} audit entries (up to #{last})");
    }
    Ok(0)
}

fn read_secret(file: Option<&Path>, env: Option<&str>, what: &str) -> Result<String> {
    if let Some(f) = file {
        let s = std::fs::read_to_string(f).with_context(|| format!("read {}", f.display()))?;
        return Ok(s.trim_end_matches(['\r', '\n']).to_string());
    }
    if let Some(v) = env {
        return std::env::var(v).with_context(|| format!("{v} is not set"));
    }
    bail!("give {what}")
}

fn parse_role(s: &str) -> Result<AccessRole> {
    let role = match s.trim() {
        "admin" => AccessRole::Admin,
        "viewer" => AccessRole::Viewer,
        other => match other.strip_prefix("submitter:") {
            Some(account) => AccessRole::Submitter {
                account: account.to_string(),
            },
            None => bail!("a role is admin, viewer or submitter:ACCOUNT"),
        },
    };
    role.validate()?;
    Ok(role)
}

fn print_sso(v: &SsoView) {
    match &v.oidc {
        Some(o) => println!(
            "OpenID Connect: \"{}\" {} (client {}, scopes {}, groups claim {}{})",
            o.name,
            o.issuer,
            o.client_id,
            o.scopes.join(" "),
            o.groups_claim,
            if o.has_secret {
                ""
            } else {
                ", no client secret"
            }
        ),
        None => println!("OpenID Connect: off"),
    }
    match &v.ldap {
        Some(l) => println!(
            "LDAP: {}{} (users {} {}, groups from {}{})",
            l.url,
            if l.starttls { " with StartTLS" } else { "" },
            l.user_base,
            l.user_filter,
            l.group_attr,
            if l.group_base.is_empty() {
                String::new()
            } else {
                format!(" and {} {}", l.group_base, l.group_filter)
            }
        ),
        None => println!("LDAP: off"),
    }
    println!("sessions last {} h; {} live", v.session_hours, v.sessions);
    if v.maps.is_empty() {
        println!("no rules: nobody can sign in (pumasi sso map group:NAME viewer)");
    } else {
        println!("{:<44} ROLE", "RULE");
        for m in &v.maps {
            println!("{:<44} {}", m.subject.to_string(), m.role);
        }
    }
}

pub async fn scim(home: &Path, cmd: ScimCmd) -> Result<i32> {
    match cmd {
        ScimCmd::Token { label, admin } => {
            let AdminResponse::ScimToken { token, view } =
                call(home, &admin, AdminOp::ScimTokenCreate { label }).await?
            else {
                bail!("unexpected response (is the control plane older than 1.12?)");
            };
            eprintln!(
                "SCIM token {} ({}) created; it is shown only now. In the identity provider, set the tenant URL to <dashboard URL>/scim/v2 and the secret token to:",
                view.id, view.label
            );
            eprintln!();
            println!("{token}");
            Ok(0)
        }
        ScimCmd::Revoke { id, admin } => {
            let AdminResponse::ScimView(_) =
                call(home, &admin, AdminOp::ScimTokenRevoke { id: id.clone() }).await?
            else {
                bail!("unexpected response");
            };
            eprintln!("SCIM token {id} revoked");
            Ok(0)
        }
        ScimCmd::Status { json, admin } => {
            let AdminResponse::ScimView(v) = call(home, &admin, AdminOp::ScimView).await? else {
                bail!("unexpected response (is the control plane older than 1.12?)");
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&*v)?);
                return Ok(0);
            }
            println!("tokens:");
            if v.tokens.is_empty() {
                println!("  none (pumasi sso scim token LABEL)");
            }
            for t in &v.tokens {
                println!(
                    "  {}  {}  created {} UTC, last used {}",
                    t.id,
                    t.label,
                    utc(t.created_unix),
                    if t.last_used_unix == 0 {
                        "never".to_string()
                    } else {
                        format!("{} UTC", utc(t.last_used_unix))
                    }
                );
            }
            println!("people ({}):", v.users.len());
            for u in &v.users {
                println!(
                    "  {:<32} {:<8} sessions {}{}",
                    u.user_name,
                    if u.active { "active" } else { "OFF" },
                    u.sessions,
                    if u.groups.is_empty() {
                        String::new()
                    } else {
                        format!("  groups: {}", u.groups.join(", "))
                    }
                );
            }
            println!("groups ({}):", v.groups.len());
            for g in &v.groups {
                println!("  {:<32} {} member(s)", g.display_name, g.members);
            }
            Ok(0)
        }
    }
}

pub async fn sso(home: &Path, cmd: SsoCmd) -> Result<i32> {
    let (admin, op) = match cmd {
        SsoCmd::Scim(cmd) => return scim(home, cmd).await,
        SsoCmd::Status { admin } => (admin, AdminOp::SsoGet),
        SsoCmd::Oidc {
            issuer,
            client_id,
            client_secret_file,
            client_secret_env,
            name,
            scopes,
            groups_claim,
            off,
            admin,
        } => {
            let u = if off {
                SsoUpdate::Oidc(None)
            } else {
                let secret = read_secret(
                    client_secret_file.as_deref(),
                    client_secret_env.as_deref(),
                    "--client-secret-file or --client-secret-env",
                )?;
                let o = OidcSettings {
                    name,
                    issuer: issuer.unwrap_or_default().trim_end_matches('/').to_string(),
                    client_id: client_id.unwrap_or_default(),
                    client_secret: Hidden(secret),
                    scopes,
                    groups_claim,
                };
                o.validate()?;
                SsoUpdate::Oidc(Some(Box::new(o)))
            };
            (admin, AdminOp::SsoSet(Box::new(u)))
        }
        SsoCmd::Ldap {
            url,
            starttls,
            bind_dn,
            bind_password_file,
            user_base,
            user_filter,
            group_attr,
            group_base,
            group_filter,
            off,
            admin,
        } => {
            let u = if off {
                SsoUpdate::Ldap(None)
            } else {
                let bind_password = match &bind_password_file {
                    Some(f) => read_secret(Some(f), None, "")?,
                    None if bind_dn.is_empty() => String::new(),
                    None => bail!("--bind-dn needs --bind-password-file"),
                };
                let l = LdapSettings {
                    url: url.unwrap_or_default(),
                    starttls,
                    bind_dn,
                    bind_password: Hidden(bind_password),
                    user_base: user_base.unwrap_or_default(),
                    user_filter,
                    group_attr,
                    group_filter: if group_base.is_empty() {
                        String::new()
                    } else {
                        group_filter
                    },
                    group_base,
                };
                l.validate()?;
                SsoUpdate::Ldap(Some(Box::new(l)))
            };
            (admin, AdminOp::SsoSet(Box::new(u)))
        }
        SsoCmd::Map { rule, role, admin } => (
            admin,
            AdminOp::SsoSet(Box::new(SsoUpdate::Map(RoleMap {
                subject: rule.parse::<Subject>()?,
                role: parse_role(&role)?,
            }))),
        ),
        SsoCmd::Unmap { rule, admin } => (
            admin,
            AdminOp::SsoSet(Box::new(SsoUpdate::Unmap(rule.parse::<Subject>()?))),
        ),
        SsoCmd::Device { state, admin } => (
            admin,
            AdminOp::SsoSet(Box::new(SsoUpdate::Device(state == "on"))),
        ),
        SsoCmd::Session { length, admin } => {
            let ms = pumasi_proto::job::parse_duration_ms(&length)?;
            let hours = (ms / 3_600_000).max(1);
            (
                admin,
                AdminOp::SsoSet(Box::new(SsoUpdate::SessionHours(
                    u32::try_from(hours).unwrap_or(u32::MAX),
                ))),
            )
        }
        SsoCmd::Ticket { admin } => {
            let t = load_ticket(home, &admin)?;
            let s = SignInTicket {
                cluster: t.cluster,
                replicas: t.replicas,
            };
            eprintln!("people sign in with (it holds no secret):");
            eprintln!();
            println!("pumasi login --sso {s}");
            return Ok(0);
        }
    };
    let AdminResponse::Sso(v) = call(home, &admin, op).await? else {
        bail!("unexpected response");
    };
    print_sso(&v);
    if v.oidc.is_some()
        && let Ok(AdminResponse::SsoInfo(i)) = call(home, &admin, AdminOp::SsoInfo).await
    {
        println!(
            "sign-in from a terminal (pumasi login --sso): {}",
            if i.device {
                "on"
            } else {
                "off (pumasi sso device on)"
            }
        );
    }
    Ok(0)
}

pub async fn admin(home: &Path, cmd: AdminCmd) -> Result<i32> {
    match cmd {
        AdminCmd::Rotate { admin } => {
            let AdminResponse::AdminTicket { ticket } =
                call(home, &admin, AdminOp::RotateAdminSecret).await?
            else {
                bail!("unexpected response");
            };
            let path = home.join(ADMIN_TICKET_FILE);
            pumasi_proto::net::write_private(&path, ticket.as_bytes())?;
            eprintln!(
                "the admin secret was replaced: older admin tickets no longer work. The new one is saved to {} (and the cluster directory); other machines run:",
                path.display()
            );
            eprintln!();
            println!("pumasi login {ticket}");
            Ok(0)
        }
    }
}

/// `pumasi login --sso` or `--ldap`: signs in and saves the session's
/// ticket like `pumasi login`.
pub async fn login(home: &Path, sign_in: &str, ldap_user: Option<String>) -> Result<i32> {
    let t: SignInTicket = sign_in
        .parse()
        .context("expected a sign-in ticket (pumasi sso ticket) or an admin or access ticket")?;
    let anon = t.anonymous();
    let client = AdminClient::connect(&anon, net_for_all(&anon.candidates(), None)).await?;
    let resp = match ldap_user {
        Some(user) => {
            let password = match std::env::var("PUMASI_PASSWORD") {
                Ok(p) => p,
                Err(_) => rpassword::prompt_password(format!("LDAP password for {user}: "))?,
            };
            client
                .call(AdminOp::LdapLogin {
                    user,
                    password: Hidden(password),
                })
                .await?
        }
        None => device_flow(&client).await?,
    };
    client.close().await;
    let AdminResponse::AccessTicket { ticket, view } = resp else {
        bail!("unexpected response");
    };
    let path = home.join(ADMIN_TICKET_FILE);
    pumasi_proto::net::write_private(&path, ticket.as_bytes())?;
    eprintln!(
        "signed in as {} ({}) until {} UTC; the session ticket is saved to {}",
        view.name,
        view.role,
        view.expires_unix
            .map(utc)
            .unwrap_or_else(|| "revoked".into()),
        path.display()
    );
    Ok(0)
}

async fn device_flow(client: &AdminClient) -> Result<AdminResponse> {
    let AdminResponse::SsoDevice(d) = client.call(AdminOp::SsoDeviceStart).await? else {
        bail!("unexpected response");
    };
    eprintln!();
    match &d.verification_uri_complete {
        Some(u) => eprintln!("open {u}"),
        None => eprintln!("open {}", d.verification_uri),
    }
    eprintln!("and enter the code  {}", d.user_code);
    eprintln!();
    eprintln!("waiting (up to {} min)...", d.expires_secs.div_ceil(60));
    let mut wait = d.interval_secs.max(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(d.expires_secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(wait)) => {}
            _ = crate::ctrl_c() => bail!("cancelled"),
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the code expired; run pumasi login --sso again"
        );
        match client
            .call(AdminOp::SsoDevicePoll {
                handle: d.handle.clone(),
            })
            .await?
        {
            AdminResponse::SsoPending { interval_secs } => wait = interval_secs.max(1),
            other => return Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles() {
        assert_eq!(parse_role("admin").unwrap(), AccessRole::Admin);
        assert_eq!(
            parse_role("submitter:ml").unwrap(),
            AccessRole::Submitter {
                account: "ml".into()
            }
        );
        assert!(parse_role("submitter:").is_err());
        assert!(parse_role("root").is_err());
    }
}
