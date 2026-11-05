//! `pumasi service`: run the agent as an OS service that starts at boot
//! and restarts when it stops (systemd on Linux, launchd on macOS, the
//! service manager on Windows).
//!
//! The service runs `pumasi --home <home> agent start <agent args>` (on
//! Windows `... service run -- <agent args>`, which speaks the service
//! control protocol). When the agent installs an upgrade it restarts in
//! place: `exec` on Unix, and on Windows the service exits with
//! [`RESTART_EXIT_CODE`], which the failure actions set at install turn
//! into a restart.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

/// Exit code of an agent that must restart (after an upgrade or a
/// rollback). `EX_TEMPFAIL` from sysexits.h.
pub const RESTART_EXIT_CODE: i32 = 75;

// launchd uses its own label (`pumasi.agent`).
#[cfg_attr(target_os = "macos", allow(dead_code))]
const NAME: &str = "pumasi-agent";
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const LAUNCHD_LABEL: &str = "pumasi.agent";

#[derive(Subcommand)]
pub enum ServiceCmd {
    /// Install and start the agent as a service that starts at boot. The
    /// machine must have joined a cluster before, or pass --ticket.
    Install {
        /// Join the cluster with this ticket when the service first starts.
        #[arg(long)]
        ticket: Option<String>,
        #[command(flatten)]
        scope: ScopeArgs,
        /// Print the service definition instead of installing it.
        #[arg(long)]
        dry_run: bool,
        /// Options for the agent, as for `pumasi agent start`, after `--`
        /// (for example `-- --allow-microvm --auto-upgrade`).
        #[arg(last = true)]
        agent_args: Vec<String>,
    },
    /// Stop and remove the service. The machine stays a cluster member.
    Uninstall {
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Show whether the service is installed and running.
    Status {
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Entry point the Windows service manager runs.
    #[command(hide = true)]
    Run {
        #[arg(last = true)]
        agent_args: Vec<String>,
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
    pub exe: PathBuf,
    pub home: PathBuf,
    pub agent_args: Vec<String>,
}

impl Spec {
    /// Arguments after the executable.
    fn args(&self) -> Vec<String> {
        let mut a = vec![
            "--home".to_string(),
            self.home.to_string_lossy().into_owned(),
        ];
        if cfg!(windows) {
            a.extend(["service".into(), "run".into(), "--".into()]);
        } else {
            a.extend(["agent".into(), "start".into()]);
        }
        a.extend(self.agent_args.iter().cloned());
        a
    }

    fn log_file(&self) -> PathBuf {
        self.home.join("logs").join("agent.log")
    }
}

/// Makes a path absolute without resolving symlinks it may not have yet.
pub fn absolute(p: &Path) -> Result<PathBuf> {
    Ok(if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    })
}

pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
    imp::install(spec, scope, dry_run)
}

pub fn uninstall(scope: ScopeArgs) -> Result<i32> {
    imp::uninstall(scope)
}

pub fn status(scope: ScopeArgs) -> Result<i32> {
    imp::status(scope)
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
    format!(
        "[Unit]
Description=Pumasi node agent
Wants=network-online.target
After=network-online.target

[Service]
ExecStart={exec}
Environment=PUMASI_LOG=pumasi=info,warn
Restart=always
RestartSec=5
# Stopping the service also stops its tasks (containers, microVMs).
KillMode=control-group
TimeoutStopSec=30

[Install]
WantedBy={}
",
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

    fn unit_path(scope: Scope) -> Result<PathBuf> {
        Ok(match scope {
            Scope::System => PathBuf::from(format!("/etc/systemd/system/{NAME}.service")),
            Scope::User => {
                let config = std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
                    .context("cannot find the user's config directory")?;
                config.join("systemd/user").join(format!("{NAME}.service"))
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
    fn installed_scope(scope: ScopeArgs) -> Result<Scope> {
        if let Some(s) = scope.explicit() {
            return Ok(s);
        }
        for s in [Scope::System, Scope::User] {
            if unit_path(s)?.exists() {
                return Ok(s);
            }
        }
        Ok(scope.resolve())
    }

    pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let scope = scope.resolve();
        let unit = systemd_unit(spec, scope == Scope::User);
        let path = unit_path(scope)?;
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
        systemctl(scope, &["enable", &format!("{NAME}.service")])?;
        systemctl(scope, &["restart", &format!("{NAME}.service")])?;
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
            "logs: journalctl {}-u {NAME} -f",
            if scope == Scope::User { "--user " } else { "" }
        );
        Ok(0)
    }

    pub fn uninstall(scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(scope)?;
        let path = unit_path(scope)?;
        if !path.exists() {
            println!("not installed ({} does not exist)", path.display());
            return Ok(0);
        }
        let _ = systemctl(scope, &["disable", "--now", &format!("{NAME}.service")]);
        std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        systemctl(scope, &["daemon-reload"])?;
        println!("removed {}", path.display());
        Ok(0)
    }

    pub fn status(scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(scope)?;
        let path = unit_path(scope)?;
        if !path.exists() {
            println!("not installed");
            return Ok(3);
        }
        let mut args = vec!["show", "-p", "ActiveState,SubState,MainPID,UnitFileState"];
        let unit = format!("{NAME}.service");
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
fn launchd_plist(spec: &Spec) -> String {
    let mut args = format!(
        "        <string>{}</string>\n",
        xml_escape(&spec.exe.to_string_lossy())
    );
    for a in spec.args() {
        args.push_str(&format!("        <string>{}</string>\n", xml_escape(&a)));
    }
    let log = xml_escape(&spec.log_file().to_string_lossy());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LAUNCHD_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{args}    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PUMASI_LOG</key>
        <string>pumasi=info,warn</string>
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

    fn plist_path(scope: Scope) -> Result<PathBuf> {
        Ok(match scope {
            Scope::System => PathBuf::from(format!("/Library/LaunchDaemons/{LAUNCHD_LABEL}.plist")),
            Scope::User => {
                let home = std::env::var_os("HOME").context("HOME is not set")?;
                PathBuf::from(home)
                    .join("Library/LaunchAgents")
                    .join(format!("{LAUNCHD_LABEL}.plist"))
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

    fn installed_scope(scope: ScopeArgs) -> Result<Scope> {
        if let Some(s) = scope.explicit() {
            return Ok(s);
        }
        for s in [Scope::System, Scope::User] {
            if plist_path(s)?.exists() {
                return Ok(s);
            }
        }
        Ok(scope.resolve())
    }

    fn unload(scope: Scope, path: &Path) {
        let target = format!("{}/{LAUNCHD_LABEL}", domain(scope));
        let ok = run_cmd("launchctl", &["bootout", &target]).is_ok_and(|o| o.status.success());
        if !ok {
            let _ = run_cmd("launchctl", &["unload", "-w", &path.to_string_lossy()]);
        }
    }

    pub fn install(spec: &Spec, scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let scope = scope.resolve();
        let plist = launchd_plist(spec);
        let path = plist_path(scope)?;
        if dry_run {
            println!("<!-- {} -->", path.display());
            print!("{plist}");
            return Ok(0);
        }
        if let Some(dir) = spec.log_file().parent() {
            std::fs::create_dir_all(dir)?;
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        if path.exists() {
            unload(scope, &path);
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

    pub fn uninstall(scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(scope)?;
        let path = plist_path(scope)?;
        if !path.exists() {
            println!("not installed ({} does not exist)", path.display());
            return Ok(0);
        }
        unload(scope, &path);
        std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        println!("removed {}", path.display());
        Ok(0)
    }

    pub fn status(scope: ScopeArgs) -> Result<i32> {
        let scope = installed_scope(scope)?;
        let path = plist_path(scope)?;
        if !path.exists() {
            println!("not installed");
            return Ok(3);
        }
        println!("service:  {}", path.display());
        let target = format!("{}/{LAUNCHD_LABEL}", domain(scope));
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
        let out = run_cmd("launchctl", &["list", LAUNCHD_LABEL])?;
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

    pub fn install(spec: &Spec, _scope: ScopeArgs, dry_run: bool) -> Result<i32> {
        let args: Vec<OsString> = spec.args().into_iter().map(OsString::from).collect();
        if dry_run {
            println!("service {NAME}");
            println!("  binary: {}", spec.exe.display());
            println!("  arguments: {:?}", spec.args());
            println!("  start: automatic, restart on failure");
            println!("  firewall: allow inbound UDP on private and domain networks");
            return Ok(0);
        }
        let mgr = manager(ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE)?;
        // Replace an existing installation.
        if let Ok(old) = mgr.open_service(
            NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        ) {
            stop_and_wait(&old);
            old.delete().context("remove the old service")?;
            drop(old);
            std::thread::sleep(Duration::from_secs(1));
        }
        let info = ServiceInfo {
            name: OsString::from(NAME),
            display_name: OsString::from("Pumasi agent"),
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
        service.set_description("Contributes this machine to a Pumasi cluster.")?;
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
        println!("installed and started the {NAME} service");
        allow_through_firewall(&spec.exe);
        println!("logs: {}", spec.log_file().display());
        Ok(0)
    }

    const FIREWALL_RULE: &str = "Pumasi agent (UDP in)";

    /// Lets the agent receive UDP on private and domain networks, which
    /// direct (hole-punched) QUIC connections need. A service gets no
    /// firewall prompt, so without a rule its traffic goes through the
    /// relay. Public networks stay closed; see the README to open them.
    fn allow_through_firewall(exe: &Path) {
        use std::os::windows::process::CommandExt;
        remove_firewall_rule();
        let out = std::process::Command::new("netsh")
            .args(["advfirewall", "firewall", "add", "rule"])
            .raw_arg(format!("name=\"{FIREWALL_RULE}\""))
            .args(["dir=in", "action=allow", "protocol=UDP", "enable=yes"])
            .arg("profile=private,domain")
            .raw_arg(format!("program=\"{}\"", exe.display()))
            .output();
        match out {
            Ok(o) if o.status.success() => println!(
                "firewall: allowed inbound UDP for {} on private and domain networks (rule {FIREWALL_RULE:?})",
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
    fn remove_firewall_rule() -> bool {
        use std::os::windows::process::CommandExt;
        std::process::Command::new("netsh")
            .args(["advfirewall", "firewall", "delete", "rule"])
            .raw_arg(format!("name=\"{FIREWALL_RULE}\""))
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

    pub fn uninstall(_scope: ScopeArgs) -> Result<i32> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let Ok(service) = mgr.open_service(
            NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        ) else {
            println!("not installed");
            return Ok(0);
        };
        stop_and_wait(&service);
        service.delete().context("remove the service")?;
        println!("removed the {NAME} service");
        if remove_firewall_rule() {
            println!("removed the firewall rule {FIREWALL_RULE:?}");
        }
        Ok(0)
    }

    pub fn status(_scope: ScopeArgs) -> Result<i32> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let Ok(service) = mgr.open_service(NAME, ServiceAccess::QUERY_STATUS) else {
            println!("not installed");
            return Ok(3);
        };
        let s = service.query_status()?;
        println!("service:  {NAME}");
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

    static ARGS: OnceLock<(PathBuf, Vec<String>)> = OnceLock::new();
    static EXIT: Mutex<i32> = Mutex::new(0);

    define_windows_service!(ffi_service_main, service_main);

    /// Hands the process to the service manager; returns when the service
    /// stops.
    pub fn run(home: PathBuf, agent_args: Vec<String>) -> i32 {
        let _ = ARGS.set((home, agent_args));
        if let Err(e) = service_dispatcher::start(NAME, ffi_service_main) {
            eprintln!(
                "error: `service run` is for the Windows service manager; use `pumasi agent start` ({e})"
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
        let status = service_control_handler::register(NAME, handler)?;
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
        let (home, args) = ARGS.get().cloned().unwrap_or_default();
        let rt = tokio::runtime::Runtime::new()?;
        let code = rt.block_on(crate::run_service_agent(home, args, async {
            let _ = stop_rx.await;
        }));
        let code = match code {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("agent failed: {e:#}");
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
            exe: PathBuf::from("/opt/pumasi dir/pumasi"),
            home: PathBuf::from("/var/lib/pumasi"),
            agent_args: vec!["--name".into(), "100% \"node\" $1".into()],
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
    }

    #[test]
    fn launchd_plists_escape_arguments() {
        let mut s = spec();
        s.agent_args = vec!["--name".into(), "a<b&c".into()];
        let plist = launchd_plist(&s);
        assert!(plist.contains("<string>a&lt;b&amp;c</string>"));
        assert!(plist.contains("<string>/opt/pumasi dir/pumasi</string>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }
}
