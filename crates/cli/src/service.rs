//! `pumasi service`: run the agent, the control plane or the dashboard/API
//! as an OS service that starts at boot and restarts when it stops
//! (systemd on Linux, launchd on macOS, the service manager on Windows).
//!
//! The service runs `pumasi --home <home> agent start <args>` (or
//! `cluster start`, or `ui`; on Windows `... service run --role <role> --
//! <args>`, which speaks the service control protocol). When the agent
//! installs an upgrade it restarts in place: `exec` on Unix, and on
//! Windows the service exits with [`RESTART_EXIT_CODE`], which the failure
//! actions set at install turn into a restart.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

/// Exit code of an agent that must restart (after an upgrade or a
/// rollback). `EX_TEMPFAIL` from sysexits.h.
pub const RESTART_EXIT_CODE: i32 = 75;

/// What a service runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Role {
    /// The node agent (`pumasi agent start`).
    Agent,
    /// The control plane (`pumasi cluster start`).
    Cluster,
    /// The dashboard or, with `-- --api-only`, the remote API (`pumasi ui`).
    Ui,
}

impl Role {
    pub const ALL: [Role; 3] = [Role::Agent, Role::Cluster, Role::Ui];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Agent => "agent",
            Role::Cluster => "cluster",
            Role::Ui => "ui",
        }
    }

    /// The service's name: systemd unit and Windows service.
    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub fn name(self) -> &'static str {
        match self {
            Role::Agent => "pumasi-agent",
            Role::Cluster => "pumasi-cluster",
            Role::Ui => "pumasi-ui",
        }
    }

    /// The launchd label.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    fn label(self) -> &'static str {
        match self {
            Role::Agent => "pumasi.agent",
            Role::Cluster => "pumasi.cluster",
            Role::Ui => "pumasi.ui",
        }
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    fn title(self) -> &'static str {
        match self {
            Role::Agent => "Pumasi agent",
            Role::Cluster => "Pumasi control plane",
            Role::Ui => "Pumasi dashboard",
        }
    }

    #[cfg_attr(not(windows), allow(dead_code))]
    fn description(self) -> &'static str {
        match self {
            Role::Agent => "Contributes this machine to a Pumasi cluster.",
            Role::Cluster => "Runs the control plane of a Pumasi cluster.",
            Role::Ui => "Serves the Pumasi dashboard or remote API.",
        }
    }

    /// The log file, where the service manager does not keep logs itself
    /// (launchd, Windows).
    pub fn log_file(self, home: &Path) -> PathBuf {
        home.join("logs").join(format!("{}.log", self.as_str()))
    }

    /// The subcommand the service runs.
    fn command(self) -> &'static [&'static str] {
        match self {
            Role::Agent => &["agent", "start"],
            Role::Cluster => &["cluster", "start"],
            Role::Ui => &["ui"],
        }
    }
}

#[derive(Subcommand)]
pub enum ServiceCmd {
    /// Install and start a service that starts at boot: the agent
    /// (default), the control plane (`cluster`) or the dashboard or
    /// remote API (`ui`). For the agent, the machine must have joined a
    /// cluster before, or pass --ticket.
    Install {
        /// What the service runs: `agent`, `cluster` (`pumasi cluster
        /// start`) or `ui` (`pumasi ui`).
        #[arg(value_enum, default_value_t = Role::Agent)]
        role: Role,
        /// Agent only: join the cluster with this ticket when the service
        /// first starts.
        #[arg(long)]
        ticket: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
        /// Print the service definition instead of installing it.
        #[arg(long)]
        dry_run: bool,
        /// Options for the command the service runs, after `--`: as for
        /// `pumasi agent start` (`-- --allow-microvm --auto-upgrade`),
        /// `pumasi cluster start` (`-- --name home`) or `pumasi ui`
        /// (`-- --api-only`).
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Stop and remove a service. An agent's machine stays a cluster
    /// member; a control plane keeps its data.
    Uninstall {
        #[arg(value_enum, default_value_t = Role::Agent)]
        role: Role,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Show whether a service is installed and running (without a role,
    /// every Pumasi service installed here).
    Status {
        #[arg(value_enum)]
        role: Option<Role>,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Entry point the Windows service manager runs.
    #[command(hide = true)]
    Run {
        #[arg(long, value_enum, default_value_t = Role::Agent)]
        role: Role,
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[derive(Args, Clone, Copy, Default)]
pub struct ScopeArgs {
    /// A service of the current user (systemd --user, a LaunchAgent that
    /// starts at login) instead of a system service. The default is a
    /// system service when run as root, else a user service. Windows
    /// services are always system services.
    #[arg(long, conflicts_with = "system")]
    user: bool,
    /// A system service (needs root): starts at boot without a login.
    #[arg(long)]
    system: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(windows, allow(dead_code))]
enum Scope {
    System,
    User,
}

impl ScopeArgs {
    #[cfg_attr(windows, allow(dead_code))]
    fn resolve(self) -> Scope {
        if self.user {
            Scope::User
        } else if self.system || is_root() {
            Scope::System
        } else {
            Scope::User
        }
    }

    #[cfg_attr(windows, allow(dead_code))]
    fn explicit(self) -> Option<Scope> {
        match (self.user, self.system) {
            (true, _) => Some(Scope::User),
            (_, true) => Some(Scope::System),
            _ => None,
        }
    }
}

#[cfg(unix)]
fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn is_root() -> bool {
    false
}

/// What the service runs.
pub struct Spec {
    pub role: Role,
    pub exe: PathBuf,
    pub home: PathBuf,
    pub args: Vec<String>,
    /// The account a system service runs as (Linux, macOS); `None` for
    /// root.
    pub user: Option<String>,
}

impl Spec {
    /// Arguments after the executable.
    fn args(&self) -> Vec<String> {
        let mut a = vec![
            "--home".to_string(),
            self.home.to_string_lossy().into_owned(),
        ];
        if cfg!(windows) {
            a.extend([
                "service".into(),
                "run".into(),
                "--role".into(),
                self.role.as_str().into(),
                "--".into(),
            ]);
        } else {
            a.extend(self.role.command().iter().map(|s| s.to_string()));
        }
        a.extend(self.args.iter().cloned());
        a
    }

    fn log_file(&self) -> PathBuf {
        self.role.log_file(&self.home)
    }
}

/// The account that owns `home` (or its nearest existing parent), which a
/// system control plane or dashboard runs as so the files it writes (the
/// admin ticket) stay readable by that user. `None` when root owns it.
#[cfg(unix)]
pub fn home_owner(home: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    let uid = home
        .ancestors()
        .find_map(|p| std::fs::metadata(p).ok())?
        .uid();
    if uid == 0 {
        return None;
    }
    // SAFETY: getpwuid returns a pointer to static storage or null; it is
    // read at once, before any other call that could reuse it.
    unsafe {
        let pw = libc::getpwuid(uid);
        if pw.is_null() {
            return Some(uid.to_string());
        }
        Some(
            std::ffi::CStr::from_ptr((*pw).pw_name)
                .to_string_lossy()
                .into_owned(),
        )
    }
}

#[cfg(not(unix))]
pub fn home_owner(_home: &Path) -> Option<String> {
    None
}

/// Makes a path absolute without resolving symlinks it may not have yet.
pub fn absolute(p: &Path) -> Result<PathBuf> {
    Ok(if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    })
}

/// Whether a system service (rather than a user one) would be installed.
pub fn is_system(scope: ScopeArgs) -> bool {
    cfg!(windows) || scope.resolve() == Scope::System
}

pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
    imp::install(spec, scope, dry_run)
}

pub fn uninstall(role: Role, scope: ScopeArgs) -> Result<i32> {
    imp::uninstall(role, scope)
}

/// Exit code 0 when the service runs, 3 when it is not installed or
/// stopped; without a role, 0 when every installed one runs.
pub fn status(role: Option<Role>, scope: ScopeArgs) -> Result<i32> {
    if let Some(role) = role {
        return imp::status(role, scope, true);
    }
    let mut any = false;
    let mut all_running = true;
    for role in Role::ALL {
        if !imp::installed(role, scope)? {
            continue;
        }
        if any {
            println!();
        }
        any = true;
        println!("{}:", role.as_str());
        all_running &= imp::status(role, scope, false)? == 0;
    }
    if !any {
        println!("no Pumasi service is installed");
        return Ok(3);
    }
    Ok(if all_running { 0 } else { 3 })
}

fn run_cmd(program: &str, args: &[&str]) -> Result<std::process::Output> {
    std::process::Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("run {program}"))
}

/// Runs a command and fails with its stderr if it fails.
#[cfg_attr(windows, allow(dead_code))]
fn check_cmd(program: &str, args: &[&str]) -> Result<String> {
    let out = run_cmd(program, args)?;
    if !out.status.success() {
        bail!(
            "`{program} {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

// ---------------------------------------------------------------------------
// Linux: systemd
// ---------------------------------------------------------------------------

/// Quotes one ExecStart argument for systemd.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn systemd_quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '%' => out.push_str("%%"),
            '$' => out.push_str("$$"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn systemd_unit(spec: &Spec, user: bool) -> String {
    let mut exec = systemd_quote(&spec.exe.to_string_lossy());
    for a in spec.args() {
        exec.push(' ');
        exec.push_str(&systemd_quote(&a));
    }
    let after = match spec.role {
        // A dashboard on the control plane's machine starts after it.
        Role::Ui => " pumasi-cluster.service",
        _ => "",
    };
    let run_as = match (&spec.user, user) {
        (Some(u), false) => format!("User={u}\n"),
        _ => String::new(),
    };
    let kill = match spec.role {
        Role::Agent => "# Stopping the service also stops its tasks (containers, microVMs).\n",
        _ => "",
    };
    format!(
        "[Unit]
Description={}
Wants=network-online.target
After=network-online.target{after}

[Service]
ExecStart={exec}
{run_as}Environment=PUMASI_LOG=pumasi=info,openraft=error,warn
Restart=always
RestartSec=5
{kill}KillMode=control-group
TimeoutStopSec=30

[Install]
WantedBy={}
",
        spec.role.title(),
        if user {
            "default.target"
        } else {
            "multi-user.target"
        }
    )
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;

    fn unit_path(role: Role, scope: Scope) -> Result<PathBuf> {
        let name = role.name();
        Ok(match scope {
            Scope::System => PathBuf::from(format!("/etc/systemd/system/{name}.service")),
            Scope::User => {
                let config = std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
                    .context("cannot find the user's config directory")?;
                config.join("systemd/user").join(format!("{name}.service"))
            }
        })
    }

    fn systemctl(scope: Scope, args: &[&str]) -> Result<String> {
        let mut all = Vec::new();
        if scope == Scope::User {
            all.push("--user");
        }
        all.extend_from_slice(args);
        check_cmd("systemctl", &all)
    }

    /// The scope given, or the one whose unit file exists.
    fn installed_scope(role: Role, scope: ScopeArgs) -> Result<Scope> {
        if let Some(s) = scope.explicit() {
            return Ok(s);
        }
        for s in [Scope::System, Scope::User] {
            if unit_path(role, s)?.exists() {
                return Ok(s);
            }
        }
        Ok(scope.resolve())
    }

    pub fn installed(role: Role, scope: ScopeArgs) -> Result<bool> {
        Ok(unit_path(role, installed_scope(role, scope)?)?.exists())
    }

    pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let scope = scope.resolve();
        let unit = systemd_unit(spec, scope == Scope::User);
        let path = unit_path(spec.role, scope)?;
        let name = spec.role.name();
        if dry_run {
            println!("# {}", path.display());
            print!("{unit}");
            return Ok(0);
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        }
        std::fs::write(&path, unit).with_context(|| {
            format!(
                "write {} (a system service needs root; use --user for a user service)",
                path.display()
            )
        })?;
        systemctl(scope, &["daemon-reload"])?;
        systemctl(scope, &["enable", &format!("{name}.service")])?;
        systemctl(scope, &["restart", &format!("{name}.service")])?;
        println!("installed {} and started it", path.display());
        if scope == Scope::User {
            // Without lingering a user service starts only at login.
            let user = std::env::var("USER").unwrap_or_default();
            match run_cmd("loginctl", &["enable-linger", &user]) {
                Ok(o) if o.status.success() => {
                    println!("enabled lingering for {user}: the service starts at boot")
                }
                _ => println!(
                    "note: run `sudo loginctl enable-linger {user}` so the service starts at boot, not at login"
                ),
            }
        }
        println!(
            "logs: journalctl {}-u {name} -f",
            if scope == Scope::User { "--user " } else { "" }
        );
        Ok(0)
    }

    pub fn uninstall(role: Role, scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(role, scope)?;
        let path = unit_path(role, scope)?;
        if !path.exists() {
            println!("not installed ({} does not exist)", path.display());
            return Ok(0);
        }
        let _ = systemctl(
            scope,
            &["disable", "--now", &format!("{}.service", role.name())],
        );
        std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        systemctl(scope, &["daemon-reload"])?;
        println!("removed {}", path.display());
        Ok(0)
    }

    pub fn status(role: Role, scope: ScopeArgs, _named: bool) -> Result<i32> {
        let scope = installed_scope(role, scope)?;
        let path = unit_path(role, scope)?;
        if !path.exists() {
            println!("not installed");
            return Ok(3);
        }
        let mut args = vec!["show", "-p", "ActiveState,SubState,MainPID,UnitFileState"];
        let unit = format!("{}.service", role.name());
        args.push(&unit);
        let show = systemctl(scope, &args)?;
        let get = |k: &str| {
            show.lines()
                .find_map(|l| l.strip_prefix(&format!("{k}=")))
                .unwrap_or("?")
                .to_string()
        };
        let active = get("ActiveState");
        println!(
            "service:  {} ({})",
            path.display(),
            match scope {
                Scope::System => "system",
                Scope::User => "user",
            }
        );
        println!("state:    {active} ({})", get("SubState"));
        println!("pid:      {}", get("MainPID"));
        println!("at boot:  {}", get("UnitFileState"));
        Ok(if active == "active" { 0 } else { 3 })
    }
}

// ---------------------------------------------------------------------------
// macOS: launchd
// ---------------------------------------------------------------------------

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn launchd_plist(spec: &Spec, system: bool) -> String {
    let mut args = format!(
        "        <string>{}</string>\n",
        xml_escape(&spec.exe.to_string_lossy())
    );
    for a in spec.args() {
        args.push_str(&format!("        <string>{}</string>\n", xml_escape(&a)));
    }
    let log = xml_escape(&spec.log_file().to_string_lossy());
    let label = spec.role.label();
    let run_as = match (&spec.user, system) {
        (Some(u), true) => format!(
            "    <key>UserName</key>\n    <string>{}</string>\n",
            xml_escape(u)
        ),
        _ => String::new(),
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
{run_as}    <key>ProgramArguments</key>
    <array>
{args}    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PUMASI_LOG</key>
        <string>pumasi=info,openraft=error,warn</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>5</integer>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
</dict>
</plist>
"#
    )
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;

    fn plist_path(role: Role, scope: Scope) -> Result<PathBuf> {
        let label = role.label();
        Ok(match scope {
            Scope::System => PathBuf::from(format!("/Library/LaunchDaemons/{label}.plist")),
            Scope::User => {
                let home = std::env::var_os("HOME").context("HOME is not set")?;
                PathBuf::from(home)
                    .join("Library/LaunchAgents")
                    .join(format!("{label}.plist"))
            }
        })
    }

    fn domain(scope: Scope) -> String {
        match scope {
            Scope::System => "system".into(),
            // SAFETY: getuid has no preconditions.
            Scope::User => format!("gui/{}", unsafe { libc::getuid() }),
        }
    }

    fn installed_scope(role: Role, scope: ScopeArgs) -> Result<Scope> {
        if let Some(s) = scope.explicit() {
            return Ok(s);
        }
        for s in [Scope::System, Scope::User] {
            if plist_path(role, s)?.exists() {
                return Ok(s);
            }
        }
        Ok(scope.resolve())
    }

    pub fn installed(role: Role, scope: ScopeArgs) -> Result<bool> {
        Ok(plist_path(role, installed_scope(role, scope)?)?.exists())
    }

    fn unload(role: Role, scope: Scope, path: &Path) {
        let target = format!("{}/{}", domain(scope), role.label());
        let ok = run_cmd("launchctl", &["bootout", &target]).is_ok_and(|o| o.status.success());
        if !ok {
            let _ = run_cmd("launchctl", &["unload", "-w", &path.to_string_lossy()]);
        }
    }

    pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let scope = scope.resolve();
        let plist = launchd_plist(spec, scope == Scope::System);
        let path = plist_path(spec.role, scope)?;
        if dry_run {
            println!("<!-- {} -->", path.display());
            print!("{plist}");
            return Ok(0);
        }
        if let Some(dir) = spec.log_file().parent() {
            let fresh = !dir.exists();
            std::fs::create_dir_all(dir)?;
            // A service that runs as the home's owner writes its log there.
            if fresh
                && scope == Scope::System
                && let Some(u) = &spec.user
            {
                let _ = run_cmd("chown", &[u.as_str(), dir.to_string_lossy().as_ref()]);
            }
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        if path.exists() {
            unload(spec.role, scope, &path);
        }
        std::fs::write(&path, plist).with_context(|| {
            format!(
                "write {} (a system service needs root; use --user for a user service)",
                path.display()
            )
        })?;
        let p = path.to_string_lossy().into_owned();
        let boot = run_cmd("launchctl", &["bootstrap", &domain(scope), &p])?;
        if !boot.status.success() {
            // Older macOS, or no GUI session for a LaunchAgent.
            check_cmd("launchctl", &["load", "-w", &p]).with_context(|| {
                format!(
                    "launchctl bootstrap failed: {}",
                    String::from_utf8_lossy(&boot.stderr).trim()
                )
            })?;
        }
        println!("installed {} and started it", path.display());
        if scope == Scope::User {
            println!(
                "note: a LaunchAgent starts when you log in; install with sudo for a service that starts at boot"
            );
        }
        println!("logs: {}", spec.log_file().display());
        Ok(0)
    }

    pub fn uninstall(role: Role, scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(role, scope)?;
        let path = plist_path(role, scope)?;
        if !path.exists() {
            println!("not installed ({} does not exist)", path.display());
            return Ok(0);
        }
        unload(role, scope, &path);
        std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        println!("removed {}", path.display());
        Ok(0)
    }

    pub fn status(role: Role, scope: ScopeArgs, _named: bool) -> Result<i32> {
        let scope = installed_scope(role, scope)?;
        let path = plist_path(role, scope)?;
        if !path.exists() {
            println!("not installed");
            return Ok(3);
        }
        println!("service:  {}", path.display());
        let target = format!("{}/{}", domain(scope), role.label());
        let out = run_cmd("launchctl", &["print", &target])?;
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout);
            let get = |k: &str| {
                text.lines()
                    .find_map(|l| l.trim().strip_prefix(&format!("{k} = ")))
                    .unwrap_or("-")
                    .to_string()
            };
            let state = get("state");
            println!("state:    {state}");
            println!("pid:      {}", get("pid"));
            return Ok(if state == "running" { 0 } else { 3 });
        }
        // Older launchctl: `list <label>` prints a plist-like dict.
        let out = run_cmd("launchctl", &["list", role.label()])?;
        if !out.status.success() {
            println!("state:    not loaded");
            return Ok(3);
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let pid = text
            .lines()
            .find_map(|l| l.trim().strip_prefix("\"PID\" = "))
            .map(|p| p.trim_end_matches(';').to_string());
        match pid {
            Some(pid) => {
                println!("state:    running");
                println!("pid:      {pid}");
                Ok(0)
            }
            None => {
                println!("state:    loaded, not running");
                Ok(3)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Windows: the service control manager
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod imp {
    use std::{ffi::OsString, time::Duration};

    use windows_service::{
        service::{
            ServiceAccess, ServiceAction, ServiceActionType, ServiceErrorControl,
            ServiceFailureActions, ServiceFailureResetPeriod, ServiceInfo, ServiceStartType,
            ServiceState, ServiceType,
        },
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    use super::*;

    fn manager(access: ServiceManagerAccess) -> Result<ServiceManager> {
        ServiceManager::local_computer(None::<&str>, access)
            .context("cannot open the service manager (run from an administrator PowerShell)")
    }

    pub fn installed(role: Role, _scope: ScopeArgs) -> Result<bool> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        Ok(mgr
            .open_service(role.name(), ServiceAccess::QUERY_STATUS)
            .is_ok())
    }

    pub fn install(spec: &Spec, _scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let name = spec.role.name();
        let args: Vec<OsString> = spec.args().into_iter().map(OsString::from).collect();
        if dry_run {
            println!("service {name}");
            println!("  binary: {}", spec.exe.display());
            println!("  arguments: {:?}", spec.args());
            println!("  start: automatic, restart on failure");
            if firewall_rule(spec.role).is_some() {
                println!("  firewall: allow inbound UDP on private and domain networks");
            }
            return Ok(0);
        }
        let mgr = manager(ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE)?;
        // Replace an existing installation.
        if let Ok(old) = mgr.open_service(
            name,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        ) {
            stop_and_wait(&old);
            old.delete().context("remove the old service")?;
            drop(old);
            std::thread::sleep(Duration::from_secs(1));
        }
        let info = ServiceInfo {
            name: OsString::from(name),
            display_name: OsString::from(spec.role.title()),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: spec.exe.clone(),
            launch_arguments: args,
            dependencies: vec![],
            account_name: None,
            account_password: None,
        };
        let service = mgr
            .create_service(
                &info,
                ServiceAccess::CHANGE_CONFIG | ServiceAccess::START | ServiceAccess::QUERY_STATUS,
            )
            .context("create the service")?;
        service.set_description(spec.role.description())?;
        // Restart whenever the agent exits with an error, including the
        // restart code it uses after an upgrade.
        let restart = ServiceAction {
            action_type: ServiceActionType::Restart,
            delay: Duration::from_secs(2),
        };
        service.update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(3600)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![restart.clone(), restart.clone(), restart]),
        })?;
        service.set_failure_actions_on_non_crash_failures(true)?;
        service.start::<&str>(&[]).context("start the service")?;
        println!("installed and started the {name} service");
        if let Some(rule) = firewall_rule(spec.role) {
            allow_through_firewall(rule, &spec.exe);
        }
        println!("logs: {}", spec.log_file().display());
        Ok(0)
    }

    /// The agent and the control plane take QUIC connections; the
    /// dashboard listens on TCP, on this machine by default.
    fn firewall_rule(role: Role) -> Option<&'static str> {
        match role {
            Role::Agent => Some("Pumasi agent (UDP in)"),
            Role::Cluster => Some("Pumasi control plane (UDP in)"),
            Role::Ui => None,
        }
    }

    /// Lets the service receive UDP on private and domain networks, which
    /// direct (hole-punched) QUIC connections need. A service gets no
    /// firewall prompt, so without a rule its traffic goes through the
    /// relay. Public networks stay closed; see the README to open them.
    fn allow_through_firewall(rule: &str, exe: &Path) {
        use std::os::windows::process::CommandExt;
        remove_firewall_rule(rule);
        let out = std::process::Command::new("netsh")
            .args(["advfirewall", "firewall", "add", "rule"])
            .raw_arg(format!("name=\"{rule}\""))
            .args(["dir=in", "action=allow", "protocol=UDP", "enable=yes"])
            .arg("profile=private,domain")
            .raw_arg(format!("program=\"{}\"", exe.display()))
            .output();
        match out {
            Ok(o) if o.status.success() => println!(
                "firewall: allowed inbound UDP for {} on private and domain networks (rule {rule:?})",
                exe.display()
            ),
            Ok(o) => println!(
                "firewall: could not add a rule ({}); direct connections may fall back to the relay",
                String::from_utf8_lossy(&o.stdout).trim()
            ),
            Err(e) => println!(
                "firewall: could not run netsh ({e}); direct connections may fall back to the relay"
            ),
        }
    }

    /// True if a rule was there.
    fn remove_firewall_rule(rule: &str) -> bool {
        use std::os::windows::process::CommandExt;
        std::process::Command::new("netsh")
            .args(["advfirewall", "firewall", "delete", "rule"])
            .raw_arg(format!("name=\"{rule}\""))
            .output()
            .is_ok_and(|o| o.status.success())
    }

    fn stop_and_wait(service: &windows_service::service::Service) {
        if let Ok(s) = service.query_status()
            && s.current_state != ServiceState::Stopped
        {
            let _ = service.stop();
            for _ in 0..60 {
                std::thread::sleep(Duration::from_millis(500));
                if service
                    .query_status()
                    .is_ok_and(|s| s.current_state == ServiceState::Stopped)
                {
                    break;
                }
            }
        }
    }

    pub fn uninstall(role: Role, _scope: ScopeArgs) -> Result<i32> {
        let name = role.name();
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let Ok(service) = mgr.open_service(
            name,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        ) else {
            println!("not installed");
            return Ok(0);
        };
        stop_and_wait(&service);
        service.delete().context("remove the service")?;
        println!("removed the {name} service");
        if let Some(rule) = firewall_rule(role)
            && remove_firewall_rule(rule)
        {
            println!("removed the firewall rule {rule:?}");
        }
        Ok(0)
    }

    pub fn status(role: Role, _scope: ScopeArgs, _named: bool) -> Result<i32> {
        let name = role.name();
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let Ok(service) = mgr.open_service(name, ServiceAccess::QUERY_STATUS) else {
            println!("not installed");
            return Ok(3);
        };
        let s = service.query_status()?;
        println!("service:  {name}");
        println!("state:    {:?}", s.current_state);
        println!(
            "pid:      {}",
            s.process_id.map(|p| p.to_string()).unwrap_or("-".into())
        );
        Ok(if s.current_state == ServiceState::Running {
            0
        } else {
            3
        })
    }
}

/// The process the Windows service manager starts.
#[cfg(windows)]
pub mod windows {
    use std::{
        ffi::OsString,
        sync::{Mutex, OnceLock},
        time::Duration,
    };

    use windows_service::{
        define_windows_service,
        service::{
            ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
            ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
        service_dispatcher,
    };

    use super::*;

    static ARGS: OnceLock<(Role, PathBuf, Vec<String>)> = OnceLock::new();
    static EXIT: Mutex<i32> = Mutex::new(0);

    define_windows_service!(ffi_service_main, service_main);

    /// Hands the process to the service manager; returns when the service
    /// stops.
    pub fn run(role: Role, home: PathBuf, args: Vec<String>) -> i32 {
        let _ = ARGS.set((role, home, args));
        if let Err(e) = service_dispatcher::start(role.name(), ffi_service_main) {
            eprintln!(
                "error: `service run` is for the Windows service manager; use `pumasi {}` ({e})",
                role.command().join(" ")
            );
            return 1;
        }
        *EXIT.lock().unwrap()
    }

    fn service_main(_arguments: Vec<OsString>) {
        let code = match run_service() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("service failed: {e:#}");
                1
            }
        };
        *EXIT.lock().unwrap() = code;
    }

    fn run_service() -> Result<i32> {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let stop_tx = Mutex::new(Some(stop_tx));
        let handler = move |event| match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                if let Some(tx) = stop_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        };
        let (role, home, args) =
            ARGS.get()
                .cloned()
                .unwrap_or((Role::Agent, PathBuf::new(), Vec::new()));
        let status = service_control_handler::register(role.name(), handler)?;
        let report = |state, code: i32| {
            let _ = status.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: state,
                controls_accepted: if state == ServiceState::Running {
                    ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
                } else {
                    ServiceControlAccept::empty()
                },
                exit_code: if code == 0 {
                    ServiceExitCode::Win32(0)
                } else {
                    ServiceExitCode::ServiceSpecific(code as u32)
                },
                checkpoint: 0,
                wait_hint: Duration::from_secs(10),
                process_id: None,
            });
        };
        report(ServiceState::Running, 0);
        let rt = tokio::runtime::Runtime::new()?;
        let code = rt.block_on(Box::pin(crate::run_service(role, home, args, async {
            let _ = stop_rx.await;
        })));
        let code = match code {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("{} failed: {e:#}", role.as_str());
                1
            }
        };
        rt.shutdown_timeout(Duration::from_secs(5));
        report(ServiceState::Stopped, code);
        Ok(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> Spec {
        Spec {
            role: Role::Agent,
            exe: PathBuf::from("/opt/pumasi dir/pumasi"),
            home: PathBuf::from("/var/lib/pumasi"),
            args: vec!["--name".into(), "100% \"node\" $1".into()],
            user: None,
        }
    }

    #[test]
    fn systemd_units_quote_arguments() {
        let unit = systemd_unit(&spec(), false);
        assert!(unit.contains("WantedBy=multi-user.target"));
        let exec = unit
            .lines()
            .find_map(|l| l.strip_prefix("ExecStart="))
            .unwrap();
        if cfg!(windows) {
            return;
        }
        assert_eq!(
            exec,
            r#""/opt/pumasi dir/pumasi" "--home" "/var/lib/pumasi" "agent" "start" "--name" "100%% \"node\" $$1""#
        );
        assert!(systemd_unit(&spec(), true).contains("WantedBy=default.target"));
        assert!(!unit.contains("User="));
    }

    #[test]
    fn control_plane_units_run_as_the_homes_owner() {
        let mut s = spec();
        s.role = Role::Cluster;
        s.args = vec!["--no-token".into()];
        s.user = Some("stan".into());
        let unit = systemd_unit(&s, false);
        assert!(unit.contains("Description=Pumasi control plane"));
        assert!(unit.contains("\nUser=stan\n"));
        if !cfg!(windows) {
            assert!(unit.contains(r#""cluster" "start" "--no-token""#));
        }
        // A user unit runs as its user anyway.
        assert!(!systemd_unit(&s, true).contains("User="));
        s.role = Role::Ui;
        let unit = systemd_unit(&s, false);
        assert!(unit.contains("After=network-online.target pumasi-cluster.service"));
        let plist = launchd_plist(&s, true);
        assert!(plist.contains("<string>pumasi.ui</string>"));
        assert!(plist.contains("<key>UserName</key>\n    <string>stan</string>"));
        assert!(plist.contains("/var/lib/pumasi/logs/ui.log"));
        assert!(!launchd_plist(&s, false).contains("UserName"));
    }

    #[test]
    fn launchd_plists_escape_arguments() {
        let mut s = spec();
        s.args = vec!["--name".into(), "a<b&c".into()];
        let plist = launchd_plist(&s, true);
        assert!(plist.contains("<string>a&lt;b&amp;c</string>"));
        assert!(plist.contains("<string>/opt/pumasi dir/pumasi</string>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }
}
