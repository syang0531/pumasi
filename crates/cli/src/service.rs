//! `pumasi service`: run the agent, the control plane or the dashboard/API
//! as an OS service that starts at boot and restarts when it stops
//! (systemd on Linux, launchd on macOS, the service manager on Windows).
//!
//! The service runs `pumasi --home <home> service run --role <role>`,
//! which reads the role's options from a file in the home (`agent.toml`,
//! `control-plane.toml`, `ui.toml`) and runs `agent start`, `cluster
//! start` or `ui` with them (1.4); on Windows it also speaks the service
//! control protocol. `pumasi service set` changes the options in the file
//! and restarts the service. Services installed by releases before 1.4
//! carry their options in the service definition instead (`pumasi --home
//! <home> agent start <options>`): they keep working, and their first
//! `service set` moves them to the file.
//!
//! When the agent installs an upgrade it restarts in place: `exec` on
//! Unix, and on Windows the service exits with [`RESTART_EXIT_CODE`],
//! which the failure actions set at install turn into a restart.

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
    pub fn command(self) -> &'static [&'static str] {
        match self {
            Role::Agent => &["agent", "start"],
            Role::Cluster => &["cluster", "start"],
            Role::Ui => &["ui"],
        }
    }

    /// The file in the home that holds the service's options (1.4). The
    /// control plane's is not `cluster.toml`: that is what the examples
    /// call the file `pumasi apply` takes.
    pub fn options_file(self, home: &Path) -> PathBuf {
        home.join(match self {
            Role::Agent => "agent.toml",
            Role::Cluster => "control-plane.toml",
            Role::Ui => "ui.toml",
        })
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
        /// Keep the options in the service definition, as releases
        /// before 1.4 did, instead of in the options file: for testing
        /// that such services still work and can be changed.
        #[arg(long, hide = true)]
        inline_options: bool,
        /// Options for the command the service runs, after `--`: as for
        /// `pumasi agent start` (`-- --allow-microvm --auto-upgrade`),
        /// `pumasi cluster start` (`-- --name home`) or `pumasi ui`
        /// (`-- --api-only`). Change them later with `pumasi service set`.
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Change the options of an installed service and restart it. The
    /// options it has stay; the ones given after `--` are added or
    /// replaced, and `--no-OPTION` removes one: `pumasi service set --
    /// --allow-webgpu --no-allow-docker --slots 4`. `pumasi service
    /// status` shows the options.
    Set {
        #[arg(value_enum, default_value_t = Role::Agent)]
        role: Role,
        #[command(flatten)]
        scope: ScopeArgs,
        /// Print the options the service would get, and change nothing.
        #[arg(long)]
        dry_run: bool,
        /// The changes, after `--`: `--OPTION [VALUE]` adds an option or
        /// replaces its value, `--no-OPTION` removes it.
        #[arg(last = true)]
        changes: Vec<String>,
    },
    /// Stop a service and start it again. Without a role, every Pumasi
    /// service installed here: the control plane first, then the
    /// dashboard and the agent, which find it again by themselves.
    /// Restarting an agent stops its tasks; the control plane gives the
    /// ones that were running to nodes again.
    Restart {
        #[arg(value_enum)]
        role: Option<Role>,
        #[command(flatten)]
        scope: ScopeArgs,
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
    /// What an installed service runs: the role's command with the
    /// options in its options file (or, for services installed before
    /// 1.4 on Windows, the ones after `--`).
    #[command(hide = true)]
    Run {
        #[arg(long, value_enum, default_value_t = Role::Agent)]
        role: Role,
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[derive(Args, Debug, Clone, Copy, Default, PartialEq, Eq)]
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

    #[cfg_attr(windows, allow(dead_code))]
    fn of(scope: Scope) -> Self {
        ScopeArgs {
            user: scope == Scope::User,
            system: scope == Scope::System,
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
    /// `None`: the service reads its options from the options file.
    /// `Some`: they go in the service definition, the form of releases
    /// before 1.4.
    pub inline: Option<Vec<String>>,
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
        let run = [
            "service".to_string(),
            "run".into(),
            "--role".into(),
            self.role.as_str().into(),
        ];
        match &self.inline {
            None => a.extend(run),
            Some(options) => {
                if cfg!(windows) {
                    a.extend(run);
                    a.push("--".into());
                } else {
                    a.extend(self.role.command().iter().map(|s| s.to_string()));
                }
                a.extend(options.iter().cloned());
            }
        }
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
    // The options go with the service.
    let options = installed(role, scope)
        .ok()
        .flatten()
        .and_then(|i| i.definition(role).ok())
        .map(|d| role.options_file(&d.home));
    let code = imp::uninstall(role, scope)?;
    if let Some(file) = options
        && file.exists()
        && std::fs::remove_file(&file).is_ok()
    {
        println!("removed {}", file.display());
    }
    Ok(code)
}

/// The installed service, as its service manager has it: `None` when it
/// is not installed.
pub fn installed(role: Role, scope: ScopeArgs) -> Result<Option<Installed>> {
    imp::definition(role, scope)
}

/// Stops the service and starts it again.
pub fn restart(role: Role, scope: ScopeArgs) -> Result<()> {
    imp::restart(role, scope)
}

/// Fails, before anything is changed, when this user may not restart the
/// service.
pub fn can_control(role: Role, scope: ScopeArgs) -> Result<()> {
    imp::can_control(role, scope)
}

/// A service as its service manager has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installed {
    /// The binary it runs.
    pub exe: PathBuf,
    /// The arguments after the binary.
    pub argv: Vec<String>,
    /// The account it runs as, when the definition names one.
    pub user: Option<String>,
    /// Where it is installed (always the system on Windows).
    pub scope: ScopeArgs,
}

/// What a service definition says about the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    pub home: PathBuf,
    /// The options in the definition itself (a service installed before
    /// 1.4). `None`: the service reads the options file.
    pub inline: Option<Vec<String>>,
}

impl Installed {
    /// Reads `--home HOME service run --role ROLE [-- OPTIONS]` or
    /// `--home HOME agent start OPTIONS` (and the other roles' commands).
    pub fn definition(&self, role: Role) -> Result<Definition> {
        let odd = || {
            anyhow::anyhow!(
                "the {} service's definition is not one `pumasi service install` wrote: {} {}",
                role.as_str(),
                self.exe.display(),
                show_options(&self.argv)
            )
        };
        let mut rest = self.argv.as_slice();
        let home = match rest {
            [flag, home, tail @ ..] if flag == "--home" => {
                rest = tail;
                PathBuf::from(home)
            }
            _ => return Err(odd()),
        };
        let run = ["service", "run", "--role", role.as_str()];
        if rest.len() >= run.len() && rest[..run.len()].iter().eq(run.iter()) {
            let inline = match &rest[run.len()..] {
                [] => None,
                [dashes, options @ ..] if dashes == "--" => {
                    // Before 1.4 the Windows service always had the
                    // `--`, with or without options after it.
                    (!options.is_empty() || !role.options_file(&home).exists())
                        .then(|| options.to_vec())
                }
                _ => return Err(odd()),
            };
            return Ok(Definition { home, inline });
        }
        let command = role.command();
        if rest.len() >= command.len() && rest[..command.len()].iter().eq(command.iter()) {
            return Ok(Definition {
                home,
                inline: Some(rest[command.len()..].to_vec()),
            });
        }
        Err(odd())
    }
}

/// The options file: `options = ["--allow-docker", "--slots", "4"]`.
#[derive(serde::Serialize, serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct OptionsFile {
    #[serde(default)]
    options: Vec<String>,
}

/// The options in the role's options file; `None` when there is no file.
pub fn read_options(role: Role, home: &Path) -> Result<Option<Vec<String>>> {
    let path = role.options_file(home);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let file: OptionsFile =
        toml::from_str(&text).with_context(|| format!("bad options file {}", path.display()))?;
    Ok(Some(file.options))
}

/// Writes the role's options file, readable only by its owner (a
/// dashboard's options may hold an admin ticket). A file root writes into
/// a home someone else owns is given to that owner, whom a system control
/// plane or dashboard runs as.
pub fn write_options(role: Role, home: &Path, options: &[String]) -> Result<PathBuf> {
    let path = role.options_file(home);
    let body = toml::to_string(&OptionsFile {
        options: options.to_vec(),
    })
    .context("write the options")?;
    let text = format!(
        "# Options of the Pumasi {} service on this machine: what `pumasi {}` would take.\n\
         # Change them with `pumasi service set {} -- --OPTION` (`--no-OPTION` removes one),\n\
         # which also restarts the service. After editing this file by hand, restart it.\n{body}",
        role.as_str(),
        role.command().join(" "),
        role.as_str(),
    );
    std::fs::create_dir_all(home).with_context(|| format!("create {}", home.display()))?;
    // Written whole, then moved into place: a service that starts now
    // reads the old options or the new ones.
    let tmp = path.with_extension("toml.tmp");
    pumasi_proto::net::write_private(&tmp, text.as_bytes())
        .with_context(|| format!("write {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if is_root()
            && let Ok(m) = std::fs::metadata(home)
            && m.uid() != 0
        {
            let _ = std::os::unix::fs::chown(&tmp, Some(m.uid()), Some(m.gid()));
        }
    }
    std::fs::rename(&tmp, &path).with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

/// The options a service started as `service run` runs with: the ones in
/// its definition when it has any (installed before 1.4), else the file's.
pub fn run_options(role: Role, home: &Path, inline: Vec<String>) -> Result<Vec<String>> {
    if !inline.is_empty() {
        return Ok(inline);
    }
    Ok(read_options(role, home)?.unwrap_or_default())
}

/// Options on one line, for people: quoted where they hold spaces, and
/// without the value of `--admin` (a ticket).
pub fn show_options(options: &[String]) -> String {
    let mut out = Vec::new();
    let mut hide = false;
    for o in options {
        let shown = if std::mem::take(&mut hide) {
            "***".to_string()
        } else if let Some((name, _)) = o.split_once('=')
            && name == "--admin"
        {
            "--admin=***".to_string()
        } else if o.is_empty() || o.contains(char::is_whitespace) || o.contains('"') {
            format!("{o:?}")
        } else {
            o.clone()
        };
        hide = o == "--admin";
        out.push(shown);
    }
    if out.is_empty() {
        "(none)".into()
    } else {
        out.join(" ")
    }
}

/// One option of a list: its name without the dashes, and its values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Opt {
    name: String,
    values: Vec<String>,
}

/// What `command` says about its option `--name`: whether it takes a
/// value, and whether it may be given more than once.
fn option_kind(command: &clap::Command, name: &str) -> Option<(bool, bool)> {
    command
        .get_arguments()
        .find(|a| a.get_long() == Some(name) && !a.is_positional())
        .map(|a| {
            let action = a.get_action();
            (
                action.takes_values(),
                matches!(action, clap::ArgAction::Append),
            )
        })
}

/// Splits `--name value --flag --other=value` into options, by what
/// `command` says takes a value. With `changes`, `--no-NAME` and other
/// names the command does not have are let through for the caller.
fn split_options(command: &clap::Command, args: &[String], changes: bool) -> Result<Vec<Opt>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        let Some(body) = arg.strip_prefix("--").filter(|b| !b.is_empty()) else {
            bail!("{arg:?}: options start with `--` (long names only)");
        };
        let (name, inline) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };
        ensure_changeable(name)?;
        let values = match option_kind(command, name) {
            Some((true, _)) => match inline {
                Some(v) => vec![v],
                None => {
                    let v = args
                        .get(i)
                        .with_context(|| format!("--{name} needs a value"))?;
                    i += 1;
                    vec![v.clone()]
                }
            },
            Some((false, _)) => {
                if inline.is_some() {
                    bail!("--{name} takes no value");
                }
                Vec::new()
            }
            None if changes && inline.is_none() => Vec::new(),
            None => bail!("no such option: --{name}"),
        };
        out.push(Opt {
            name: name.to_string(),
            values,
        });
    }
    Ok(out)
}

fn ensure_changeable(name: &str) -> Result<()> {
    if matches!(name, "home" | "help" | "version") {
        bail!("--{name} is not an option of the service");
    }
    Ok(())
}

/// The options of a service after `changes`: the current ones stay,
/// `--NAME [VALUE]` adds one or replaces its value (an option that may
/// repeat, such as `--relay`, gets one more value), and `--no-NAME`
/// removes `--NAME`. Flags that come in pairs (`--auto-upgrade` and
/// `--no-auto-upgrade`) replace each other, and a flag that only has a
/// `--no-` form (`--no-gpus`) is removed with the plain name (`--gpus`).
/// `command` is the command the service runs.
pub fn merge_options(
    command: &clap::Command,
    current: &[String],
    changes: &[String],
) -> Result<Vec<String>> {
    let mut options = split_options(command, current, false)
        .context("the service's current options are not what this pumasi takes")?;
    let known = |name: &str| option_kind(command, name).is_some();
    for change in split_options(command, changes, true)? {
        let name = change.name.as_str();
        if let Some((_, repeats)) = option_kind(command, name) {
            // A flag and its opposite do not stay together.
            let opposite = match name.strip_prefix("no-") {
                Some(plain) => plain.to_string(),
                None => format!("no-{name}"),
            };
            if known(&opposite) {
                options.retain(|o| o.name != opposite);
            }
            if repeats {
                if !options.contains(&change) {
                    options.push(change);
                }
            } else if let Some(old) = options.iter_mut().find(|o| o.name == name) {
                *old = change;
            } else {
                options.push(change);
            }
        } else if let Some(plain) = name.strip_prefix("no-").filter(|p| known(p)) {
            // `--no-NAME`: remove `--NAME`.
            options.retain(|o| o.name != plain);
        } else if known(&format!("no-{name}")) {
            // `--NAME` where only `--no-NAME` exists: remove that.
            let no = format!("no-{name}");
            options.retain(|o| o.name != no);
        } else {
            bail!(
                "no such option: --{name} (see `pumasi {} --help`)",
                command_path(command)
            );
        }
    }
    Ok(options
        .into_iter()
        .flat_map(|o| std::iter::once(format!("--{}", o.name)).chain(o.values))
        .collect())
}

fn command_path(command: &clap::Command) -> String {
    command
        .get_bin_name()
        .unwrap_or(command.get_name())
        .trim_start_matches("pumasi ")
        .to_string()
}

/// Exit code 0 when the service runs, 3 when it is not installed or
/// stopped; without a role, 0 when every installed one runs.
pub fn status(role: Option<Role>, scope: ScopeArgs) -> Result<i32> {
    if let Some(role) = role {
        let code = imp::status(role, scope, true)?;
        print_options(role, scope);
        return Ok(code);
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
        print_options(role, scope);
    }
    if !any {
        println!("no Pumasi service is installed");
        return Ok(3);
    }
    Ok(if all_running { 0 } else { 3 })
}

/// The `options:` line of `service status`: what the service runs with,
/// and where that is kept.
fn print_options(role: Role, scope: ScopeArgs) {
    let Ok(Some(installed)) = installed(role, scope) else {
        return;
    };
    let Ok(def) = installed.definition(role) else {
        return;
    };
    let file = role.options_file(&def.home);
    match &def.inline {
        Some(options) => {
            println!("options:  {}", show_options(options));
            println!(
                "          (in the service definition, from a release before 1.4; `pumasi service set` moves them to {})",
                file.display()
            );
        }
        None => match read_options(role, &def.home) {
            Ok(options) => {
                println!("options:  {}", show_options(&options.unwrap_or_default()));
                println!("          ({})", file.display());
            }
            Err(e) => println!("options:  unreadable ({e:#})"),
        },
    }
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

/// The arguments of an `ExecStart=` line: what [`systemd_quote`] wrote,
/// and plain words.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn systemd_split(exec: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let (mut has, mut quoted) = (false, false);
    let mut chars = exec.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                has = true;
            }
            '\\' if quoted => {
                if let Some(next) = chars.next() {
                    cur.push(next);
                }
            }
            '%' | '$' if chars.peek() == Some(&c) => {
                chars.next();
                cur.push(c);
                has = true;
            }
            c if c.is_whitespace() && !quoted => {
                if std::mem::take(&mut has) {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => {
                cur.push(c);
                has = true;
            }
        }
    }
    if has {
        out.push(cur);
    }
    out
}

/// The binary, its arguments and the account of a unit file.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn systemd_definition(unit: &str) -> Option<(PathBuf, Vec<String>, Option<String>)> {
    let line = |key: &str| {
        unit.lines()
            .find_map(|l| l.trim().strip_prefix(key))
            .map(str::trim)
    };
    let mut argv = systemd_split(line("ExecStart=")?);
    if argv.is_empty() {
        return None;
    }
    let exe = PathBuf::from(argv.remove(0));
    Some((exe, argv, line("User=").map(str::to_string)))
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

    pub fn definition(role: Role, scope: ScopeArgs) -> Result<Option<Installed>> {
        let scope = installed_scope(role, scope)?;
        let path = unit_path(role, scope)?;
        let unit = match std::fs::read_to_string(&path) {
            Ok(u) => u,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let (exe, argv, user) = systemd_definition(&unit)
            .with_context(|| format!("{} has no ExecStart", path.display()))?;
        Ok(Some(Installed {
            exe,
            argv,
            user,
            scope: ScopeArgs::of(scope),
        }))
    }

    pub fn can_control(role: Role, scope: ScopeArgs) -> Result<()> {
        if installed_scope(role, scope)? == Scope::System && !is_root() {
            bail!(
                "the {} service is a system service: use sudo",
                role.as_str()
            );
        }
        Ok(())
    }

    pub fn restart(role: Role, scope: ScopeArgs) -> Result<()> {
        let scope = installed_scope(role, scope)?;
        systemctl(scope, &["restart", &format!("{}.service", role.name())])
            .context("restart the service (a system service needs root)")?;
        Ok(())
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
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The `<string>` values between `open` and `close` in `text`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_strings(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("<string>") {
        let after = &rest[start + "<string>".len()..];
        let Some(end) = after.find("</string>") else {
            break;
        };
        out.push(xml_unescape(&after[..end]));
        rest = &after[end + "</string>".len()..];
    }
    out
}

/// The binary, its arguments and the account of a plist that
/// [`launchd_plist`] wrote.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn launchd_definition(plist: &str) -> Option<(PathBuf, Vec<String>, Option<String>)> {
    let after = |key: &str| {
        let k = format!("<key>{key}</key>");
        plist.find(&k).map(|i| &plist[i + k.len()..])
    };
    let args = after("ProgramArguments")?;
    let array = &args[args.find("<array>")?..args.find("</array>")?];
    let mut argv = xml_strings(array);
    if argv.is_empty() {
        return None;
    }
    let exe = PathBuf::from(argv.remove(0));
    // The value right after the key, not a later string.
    let user = after("UserName").and_then(|rest| {
        let rest = rest.trim_start();
        rest.starts_with("<string>")
            .then(|| xml_strings(rest).into_iter().next())
            .flatten()
    });
    Some((exe, argv, user))
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

    pub fn definition(role: Role, scope: ScopeArgs) -> Result<Option<Installed>> {
        let scope = installed_scope(role, scope)?;
        let path = plist_path(role, scope)?;
        let plist = match std::fs::read_to_string(&path) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let (exe, argv, user) = launchd_definition(&plist)
            .with_context(|| format!("{} has no ProgramArguments", path.display()))?;
        Ok(Some(Installed {
            exe,
            argv,
            user,
            scope: ScopeArgs::of(scope),
        }))
    }

    pub fn can_control(role: Role, scope: ScopeArgs) -> Result<()> {
        if installed_scope(role, scope)? == Scope::System && !is_root() {
            bail!(
                "the {} service is a system service: use sudo",
                role.as_str()
            );
        }
        Ok(())
    }

    pub fn restart(role: Role, scope: ScopeArgs) -> Result<()> {
        let scope = installed_scope(role, scope)?;
        let target = format!("{}/{}", domain(scope), role.label());
        if run_cmd("launchctl", &["kickstart", "-k", &target]).is_ok_and(|o| o.status.success()) {
            return Ok(());
        }
        // Not loaded (or an older launchctl): load it again.
        let path = plist_path(role, scope)?;
        unload(role, scope, &path);
        let p = path.to_string_lossy().into_owned();
        let boot = run_cmd("launchctl", &["bootstrap", &domain(scope), &p])?;
        if !boot.status.success() {
            check_cmd("launchctl", &["load", "-w", &p])
                .context("restart the service (a system service needs root)")?;
        }
        Ok(())
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

/// Splits a Windows command line (a service's binary path with its
/// arguments) the way the C runtime does: quotes group, and backslashes
/// are literal except before a quote.
#[cfg_attr(not(windows), allow(dead_code))]
fn split_command_line(line: &str) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let (mut has, mut quoted) = (false, false);
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                let start = i;
                while i < chars.len() && chars[i] == '\\' {
                    i += 1;
                }
                let n = i - start;
                if chars.get(i) == Some(&'"') {
                    cur.extend(std::iter::repeat_n('\\', n / 2));
                    if n % 2 == 1 {
                        cur.push('"');
                        i += 1;
                    }
                } else {
                    cur.extend(std::iter::repeat_n('\\', n));
                }
                has = true;
                continue;
            }
            '"' => {
                quoted = !quoted;
                has = true;
            }
            c if c.is_whitespace() && !quoted => {
                if std::mem::take(&mut has) {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => {
                cur.push(c);
                has = true;
            }
        }
        i += 1;
    }
    if has {
        out.push(cur);
    }
    out
}

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

    pub fn definition(role: Role, scope: ScopeArgs) -> Result<Option<Installed>> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let Ok(service) = mgr.open_service(role.name(), ServiceAccess::QUERY_CONFIG) else {
            return Ok(None);
        };
        let config = service.query_config().context("read the service")?;
        // The binary path holds the whole command line.
        let mut argv = split_command_line(&config.executable_path.to_string_lossy());
        if argv.is_empty() {
            bail!("the {} service has no command line", role.name());
        }
        let exe = PathBuf::from(argv.remove(0));
        Ok(Some(Installed {
            exe,
            argv,
            user: None,
            scope,
        }))
    }

    pub fn can_control(role: Role, _scope: ScopeArgs) -> Result<()> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        mgr.open_service(
            role.name(),
            ServiceAccess::QUERY_STATUS
                | ServiceAccess::STOP
                | ServiceAccess::START
                | ServiceAccess::DELETE,
        )
        .context("changing the service needs an administrator PowerShell")?;
        Ok(())
    }

    pub fn restart(role: Role, _scope: ScopeArgs) -> Result<()> {
        let mgr = manager(ServiceManagerAccess::CONNECT)?;
        let service = mgr
            .open_service(
                role.name(),
                ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::START,
            )
            .context("open the service (run from an administrator PowerShell)")?;
        stop_and_wait(&service);
        service.start::<&str>(&[]).context("start the service")?;
        Ok(())
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
            // A service that does not stop ends itself after
            // `STOP_TIMEOUT`.
            let tries = (super::windows::STOP_TIMEOUT + Duration::from_secs(10)).as_millis() / 500;
            for _ in 0..tries {
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
        sync::{Arc, Mutex, OnceLock},
        time::Duration,
    };

    use windows_service::{
        define_windows_service,
        service::{
            ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
            ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
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

    /// How long a stop may take (the systemd units' `TimeoutStopSec`).
    /// systemd and launchd kill a service that takes longer; the Windows
    /// service manager waits for ever, so the service ends itself.
    pub(super) const STOP_TIMEOUT: Duration = Duration::from_secs(30);

    /// Set (in the service's `Environment` registry value) to make the
    /// service ignore a stop, for the test of [`STOP_TIMEOUT`].
    const STOP_HANGS_ENV: &str = "PUMASI_TEST_STOP_HANGS";

    /// What the service manager is told for `state`.
    fn service_status(state: ServiceState, code: i32) -> ServiceStatus {
        let stopping = state == ServiceState::StopPending;
        ServiceStatus {
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
            checkpoint: u32::from(stopping),
            wait_hint: if stopping {
                STOP_TIMEOUT + Duration::from_secs(5)
            } else {
                Duration::from_secs(10)
            },
            process_id: None,
        }
    }

    /// Reports the service's state to the service manager, from the main
    /// thread and from the control handler's. Nothing is reported after
    /// `Stopped`: a stop that arrives while the service ends by itself
    /// must not turn it back into "StopPending".
    struct Reporter(Mutex<Option<ServiceStatusHandle>>);

    impl Reporter {
        /// False when the service was reported as stopped before.
        fn report(&self, state: ServiceState, code: i32) -> bool {
            let mut handle = self.0.lock().unwrap();
            let Some(status) = *handle else { return false };
            let _ = status.set_service_status(service_status(state, code));
            if state == ServiceState::Stopped {
                *handle = None;
            }
            true
        }

        /// Ends the process if the service has not stopped
        /// [`STOP_TIMEOUT`] after it was asked to: as stopped, so that
        /// the failure actions do not start it again behind whoever asked.
        fn end_if_stop_hangs(self: Arc<Self>, role: Role) {
            std::thread::spawn(move || {
                std::thread::sleep(STOP_TIMEOUT);
                if self.report(ServiceState::Stopped, 0) {
                    tracing::error!(
                        "{} did not stop within {} s of being asked to; ending the process",
                        role.as_str(),
                        STOP_TIMEOUT.as_secs()
                    );
                    std::process::exit(0);
                }
            });
        }
    }

    fn run_service() -> Result<i32> {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let stop_tx = Mutex::new(Some(stop_tx));
        let reporter = Arc::new(Reporter(Mutex::new(None)));
        let stopping = reporter.clone();
        let (role, home, args) =
            ARGS.get()
                .cloned()
                .unwrap_or((Role::Agent, PathBuf::new(), Vec::new()));
        let handler = move |event| match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                // Before the stop starts, so that whoever asked reads
                // "StopPending" at once. Stopping takes seconds with nodes
                // or tasks, and PowerShell's `Restart-Service` and
                // `Stop-Service` give up on a service that still says
                // "Running" 2 s after the stop (and then never start it
                // again, while the service goes on to stop).
                stopping.report(ServiceState::StopPending, 0);
                if let Some(tx) = stop_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                    stopping.clone().end_if_stop_hangs(role);
                }
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        };
        let status = service_control_handler::register(role.name(), handler)?;
        *reporter.0.lock().unwrap() = Some(status);
        reporter.report(ServiceState::Running, 0);
        let rt = tokio::runtime::Runtime::new()?;
        let stop_hangs = std::env::var_os(STOP_HANGS_ENV).is_some();
        let code = rt.block_on(Box::pin(crate::run_service(role, home, args, async move {
            let _ = stop_rx.await;
            if stop_hangs {
                std::future::pending::<()>().await;
            }
        })));
        let code = match code {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("{} failed: {e:#}", role.as_str());
                1
            }
        };
        rt.shutdown_timeout(Duration::from_secs(5));
        reporter.report(ServiceState::Stopped, code);
        Ok(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A service as releases before 1.4 installed it: options inline.
    fn spec() -> Spec {
        Spec {
            role: Role::Agent,
            exe: PathBuf::from("/opt/pumasi dir/pumasi"),
            home: PathBuf::from("/var/lib/pumasi"),
            inline: Some(vec!["--name".into(), "100% \"node\" $1".into()]),
            user: None,
        }
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
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
        s.inline = Some(vec!["--no-token".into()]);
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
        assert!(plist.contains(&*s.log_file().to_string_lossy()));
        assert!(s.log_file().ends_with("logs/ui.log"));
        assert!(!launchd_plist(&s, false).contains("UserName"));
    }

    #[test]
    fn launchd_plists_escape_arguments() {
        let mut s = spec();
        s.inline = Some(vec!["--name".into(), "a<b&c".into()]);
        let plist = launchd_plist(&s, true);
        assert!(plist.contains("<string>a&lt;b&amp;c</string>"));
        assert!(plist.contains("<string>/opt/pumasi dir/pumasi</string>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }
    #[test]
    fn new_services_read_their_options_from_the_file() {
        let mut s = spec();
        s.inline = None;
        let unit = systemd_unit(&s, false);
        let exec = unit
            .lines()
            .find_map(|l| l.strip_prefix("ExecStart="))
            .unwrap();
        assert_eq!(
            exec,
            r#""/opt/pumasi dir/pumasi" "--home" "/var/lib/pumasi" "service" "run" "--role" "agent""#
        );
        assert_eq!(
            Role::Agent.options_file(&s.home),
            PathBuf::from("/var/lib/pumasi/agent.toml")
        );
        // Not the name the `pumasi apply` examples use.
        assert!(
            Role::Cluster
                .options_file(&s.home)
                .ends_with("control-plane.toml")
        );
        assert!(Role::Ui.options_file(&s.home).ends_with("ui.toml"));
    }

    #[test]
    fn definitions_are_read_back() {
        // systemd: what `systemd_unit` wrote, with its quoting.
        let mut s = spec();
        s.user = Some("stan".into());
        let (exe, argv, user) = systemd_definition(&systemd_unit(&s, false)).unwrap();
        assert_eq!(exe, s.exe);
        assert_eq!(argv, s.args());
        assert_eq!(user.as_deref(), Some("stan"));
        s.inline = None;
        s.user = None;
        let (exe, argv, user) = systemd_definition(&systemd_unit(&s, true)).unwrap();
        assert_eq!((exe, argv, user), (s.exe.clone(), s.args(), None));
        // A unit written by hand, without quotes.
        let (exe, argv, _) = systemd_definition(
            "[Service]\nExecStart=/usr/local/bin/pumasi --home /var/lib/pumasi agent start --slots 2\n",
        )
        .unwrap();
        assert_eq!(exe, PathBuf::from("/usr/local/bin/pumasi"));
        assert_eq!(
            argv,
            strs(&[
                "--home",
                "/var/lib/pumasi",
                "agent",
                "start",
                "--slots",
                "2"
            ])
        );
        assert!(systemd_definition("[Service]\nRestart=always\n").is_none());

        // launchd: what `launchd_plist` wrote, with its escaping; the log
        // path and the label are not arguments.
        let mut s = spec();
        s.inline = Some(strs(&["--name", "a<b&c \"d\"", "--allow-docker"]));
        s.user = Some("stan".into());
        let (exe, argv, user) = launchd_definition(&launchd_plist(&s, true)).unwrap();
        assert_eq!(exe, s.exe);
        assert_eq!(argv, s.args());
        assert_eq!(user.as_deref(), Some("stan"));
        let (_, argv, user) = launchd_definition(&launchd_plist(&s, false)).unwrap();
        assert_eq!(argv, s.args());
        assert_eq!(user, None);

        // Windows: the service's command line.
        assert_eq!(
            split_command_line(
                r#""C:\Program Files\Pumasi\pumasi.exe" --home C:\Users\stan\.pumasi service run --role agent -- --name "my node" --allow-docker"#
            ),
            strs(&[
                r"C:\Program Files\Pumasi\pumasi.exe",
                "--home",
                r"C:\Users\stan\.pumasi",
                "service",
                "run",
                "--role",
                "agent",
                "--",
                "--name",
                "my node",
                "--allow-docker",
            ])
        );
        // Backslashes are literal except before a quote.
        assert_eq!(
            split_command_line(r#"a\b "c d\\" "e \"f\"" g\\h """#),
            strs(&[r"a\b", r"c d\", r#"e "f""#, r"g\\h", ""])
        );
        assert!(split_command_line("   ").is_empty());
    }

    fn installed(argv: &[&str]) -> Installed {
        Installed {
            exe: PathBuf::from("/usr/local/bin/pumasi"),
            argv: strs(argv),
            user: None,
            scope: ScopeArgs::default(),
        }
    }

    #[test]
    fn definitions_say_where_the_options_are() {
        let home = std::env::temp_dir().join(format!("pumasi-svc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let h = home.to_string_lossy().into_owned();
        // 1.4: the options file.
        let d = installed(&["--home", &h, "service", "run", "--role", "agent"])
            .definition(Role::Agent)
            .unwrap();
        assert_eq!(d.home, home);
        assert_eq!(d.inline, None);
        // Before 1.4 on Linux and macOS: the role's command and options.
        let d = installed(&["--home", &h, "agent", "start", "--allow-docker"])
            .definition(Role::Agent)
            .unwrap();
        assert_eq!(d.inline, Some(strs(&["--allow-docker"])));
        let d = installed(&[
            "--home",
            &h,
            "ui",
            "--api-only",
            "--listen",
            "127.0.0.1:7400",
        ])
        .definition(Role::Ui)
        .unwrap();
        assert_eq!(
            d.inline,
            Some(strs(&["--api-only", "--listen", "127.0.0.1:7400"]))
        );
        let d = installed(&["--home", &h, "cluster", "start"])
            .definition(Role::Cluster)
            .unwrap();
        assert_eq!(d.inline, Some(vec![]));
        // Before 1.4 on Windows: `service run -- OPTIONS`, with the `--`
        // even without options.
        let d = installed(&[
            "--home", &h, "service", "run", "--role", "agent", "--", "--slots", "2",
        ])
        .definition(Role::Agent)
        .unwrap();
        assert_eq!(d.inline, Some(strs(&["--slots", "2"])));
        let bare = installed(&["--home", &h, "service", "run", "--role", "agent", "--"]);
        assert_eq!(bare.definition(Role::Agent).unwrap().inline, Some(vec![]));

        // The file: written private, read back, and what `service run`
        // uses unless the definition carries options.
        assert_eq!(read_options(Role::Agent, &home).unwrap(), None);
        assert!(run_options(Role::Agent, &home, vec![]).unwrap().is_empty());
        let options = strs(&["--allow-docker", "--name", "my \"node\"", "--slots", "2"]);
        let path = write_options(Role::Agent, &home, &options).unwrap();
        assert_eq!(path, home.join("agent.toml"));
        assert!(std::fs::read_to_string(&path).unwrap().starts_with("# "));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_eq!(
            read_options(Role::Agent, &home).unwrap(),
            Some(options.clone())
        );
        assert_eq!(run_options(Role::Agent, &home, vec![]).unwrap(), options);
        assert_eq!(
            run_options(Role::Agent, &home, strs(&["--slots", "9"])).unwrap(),
            strs(&["--slots", "9"])
        );
        // With a file there, a bare `--` is not a list of options.
        assert_eq!(bare.definition(Role::Agent).unwrap().inline, None);
        // A file that is not ours is an error, not no options.
        std::fs::write(home.join("ui.toml"), "option = [\"--api-only\"]\n").unwrap();
        assert!(read_options(Role::Ui, &home).is_err());
        std::fs::remove_dir_all(&home).ok();

        // Not a definition `service install` wrote.
        for odd in [
            &["agent", "start"][..],
            &["--home", &h, "agent", "join", "ticket"],
            &["--home", &h, "service", "run", "--role", "ui"],
            &["--home", &h, "service", "run", "--role", "agent", "extra"],
        ] {
            assert!(installed(odd).definition(Role::Agent).is_err(), "{odd:?}");
        }
    }

    /// Options like the agent's: flags, values, a pair and a list.
    fn agent_like() -> clap::Command {
        use clap::{Arg, ArgAction};
        let flag = |name: &'static str| Arg::new(name).long(name).action(ArgAction::SetTrue);
        let value = |name: &'static str| Arg::new(name).long(name).action(ArgAction::Set);
        clap::Command::new("start")
            .arg(value("name"))
            .arg(value("slots"))
            .arg(value("net"))
            .arg(flag("allow-docker"))
            .arg(flag("allow-microvm"))
            .arg(flag("allow-webgpu"))
            .arg(flag("auto-upgrade"))
            .arg(flag("no-auto-upgrade"))
            .arg(flag("no-gpus"))
            .arg(Arg::new("relay").long("relay").action(ArgAction::Append))
            .arg(Arg::new("home").long("home").action(ArgAction::Set))
    }

    #[test]
    fn service_set_keeps_what_it_does_not_name() {
        let cmd = agent_like();
        let merge = |current: &[&str], changes: &[&str]| {
            merge_options(&cmd, &strs(current), &strs(changes)).map(|v| v.join(" "))
        };
        // The issue's case: one more flag on an agent that has others.
        assert_eq!(
            merge(&["--allow-docker", "--net", "local"], &["--allow-webgpu"]).unwrap(),
            "--allow-docker --net local --allow-webgpu"
        );
        assert_eq!(
            merge(
                &["--allow-docker", "--allow-microvm"],
                &["--allow-webgpu", "--no-allow-docker"]
            )
            .unwrap(),
            "--allow-microvm --allow-webgpu"
        );
        // Values are replaced in place, in either spelling, or removed.
        assert_eq!(
            merge(&["--slots", "2", "--name", "my node"], &["--slots=8"]).unwrap(),
            "--slots 8 --name my node"
        );
        assert_eq!(
            merge(&["--slots=2", "--allow-docker"], &["--no-slots"]).unwrap(),
            "--allow-docker"
        );
        // The same change twice changes nothing more; removing what is
        // not there is fine.
        let once = merge(&["--allow-docker"], &["--allow-webgpu", "--slots", "4"]).unwrap();
        let again = merge_options(
            &cmd,
            &once.split(' ').map(String::from).collect::<Vec<_>>(),
            &strs(&["--allow-webgpu", "--slots", "4"]),
        )
        .unwrap()
        .join(" ");
        assert_eq!(once, again);
        assert_eq!(merge(&[], &["--no-allow-docker"]).unwrap(), "");
        // A flag and its opposite replace each other.
        assert_eq!(
            merge(&["--auto-upgrade"], &["--no-auto-upgrade"]).unwrap(),
            "--no-auto-upgrade"
        );
        assert_eq!(
            merge(&["--no-auto-upgrade"], &["--auto-upgrade"]).unwrap(),
            "--auto-upgrade"
        );
        // A flag with only a `--no-` form: the plain name removes it.
        assert_eq!(merge(&[], &["--no-gpus"]).unwrap(), "--no-gpus");
        assert_eq!(
            merge(&["--no-gpus", "--slots", "1"], &["--gpus"]).unwrap(),
            "--slots 1"
        );
        assert_eq!(merge(&["--no-gpus"], &["--no-no-gpus"]).unwrap(), "");
        // An option that repeats gets one more value; `--no-` clears it.
        assert_eq!(
            merge(
                &["--relay", "https://a"],
                &["--relay", "https://b", "--relay", "https://a"]
            )
            .unwrap(),
            "--relay https://a --relay https://b"
        );
        assert_eq!(
            merge(
                &["--relay", "https://a", "--relay", "https://b"],
                &["--no-relay", "--relay", "https://c"]
            )
            .unwrap(),
            "--relay https://c"
        );
        // Refused.
        for bad in [
            &["--frobnicate"][..],
            &["--no-frobnicate"],
            &["extra"],
            &["-s", "2"],
            &["--slots"],
            &["--allow-docker=yes"],
            &["--home", "/tmp/x"],
            &["--help"],
        ] {
            assert!(merge(&["--allow-docker"], bad).is_err(), "{bad:?}");
        }
        // Current options this pumasi does not take are an error too,
        // not silently dropped.
        assert!(merge(&["--gone"], &["--allow-webgpu"]).is_err());
    }

    #[test]
    fn shown_options_hide_tickets() {
        assert_eq!(show_options(&[]), "(none)");
        assert_eq!(
            show_options(&strs(&["--name", "my node", "--allow-docker"])),
            r#"--name "my node" --allow-docker"#
        );
        assert_eq!(
            show_options(&strs(&[
                "--api-only",
                "--admin",
                "pumasiadminsecret",
                "--no-open"
            ])),
            "--api-only --admin *** --no-open"
        );
        assert_eq!(
            show_options(&strs(&["--admin=pumasiadminsecret"])),
            "--admin=***"
        );
    }
}
