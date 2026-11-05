//! The availability policy: when a machine takes new tasks. Owners of
//! laptops and desktops that people use pick any of: only on AC power,
//! only after the keyboard and mouse have been idle for a while, only
//! within some hours of the day. The agent checks it every few seconds and
//! tells the control plane when the answer changes; a paused node gets no
//! new tasks, and tasks it is running finish.
//!
//! How each condition is read:
//!
//! | | Power | Idle time |
//! | --- | --- | --- |
//! | macOS | `pmset -g batt` | `HIDIdleTime` of `IOHIDSystem` (`ioreg`), system-wide, so it works for a LaunchDaemon |
//! | Linux | `/sys/class/power_supply` | the most recent of: terminal access times (`/dev/tty*`, `/dev/pts/*`, as `w` shows) and logind's idle hint for graphical sessions; no sessions at all counts as idle |
//! | Windows | `GetSystemPowerStatus` | `GetLastInputInfo`, which sees only its own session: a service (session 0) cannot tell |
//!
//! A machine with no battery is on AC power. Idle time that cannot be read
//! pauses the node, with the reason, rather than guessing.

use std::{fmt, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::msg::Availability;

/// What the owner allows. The default allows everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Policy {
    /// Pause while running on battery.
    pub only_on_ac: bool,
    /// Pause until the keyboard and mouse have been idle this long.
    pub idle_for: Option<Duration>,
    /// Take tasks only within these local times of day (empty: any time).
    pub hours: Vec<HourRange>,
}

impl Policy {
    /// Whether anything is restricted at all.
    pub fn is_active(&self) -> bool {
        self.only_on_ac || self.idle_for.is_some() || !self.hours.is_empty()
    }

    /// Evaluates the policy against `probe` at `minute` (minutes since
    /// local midnight).
    pub fn evaluate(&self, probe: &dyn Probe, minute: u32) -> Availability {
        let paused = |reason: String| Availability::Paused { reason };
        if !self.hours.is_empty() && !self.hours.iter().any(|h| h.contains(minute)) {
            return paused(format!("outside its hours ({})", HourList(&self.hours)));
        }
        if self.only_on_ac && probe.on_battery() == Some(true) {
            return paused("on battery power".into());
        }
        if let Some(want) = self.idle_for {
            match probe.idle_time() {
                Ok(idle) if idle >= want => {}
                Ok(idle) => {
                    return paused(format!(
                        "in use (idle {} of the {} required)",
                        fmt_duration(idle),
                        fmt_duration(want)
                    ));
                }
                Err(why) => return paused(format!("cannot tell whether it is idle: {why}")),
            }
        }
        Availability::Available
    }
}

/// `HH:MM-HH:MM` in local time. An end before the start wraps past
/// midnight (`22:00-07:00`); `24:00` is the end of the day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourRange {
    start: u32,
    end: u32,
}

impl HourRange {
    pub fn contains(&self, minute: u32) -> bool {
        if self.start <= self.end {
            (self.start..self.end).contains(&minute)
        } else {
            minute >= self.start || minute < self.end
        }
    }
}

fn parse_time(s: &str) -> Result<u32> {
    let (h, m) = s
        .split_once(':')
        .with_context(|| format!("{s:?} is not HH:MM"))?;
    let h: u32 = h.trim().parse().with_context(|| format!("hour in {s:?}"))?;
    let m: u32 = m
        .trim()
        .parse()
        .with_context(|| format!("minute in {s:?}"))?;
    ensure!(
        m < 60 && (h < 24 || (h == 24 && m == 0)),
        "{s:?} is not a time of day"
    );
    Ok(h * 60 + m)
}

impl FromStr for HourRange {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let (a, b) = s
            .split_once('-')
            .with_context(|| format!("{s:?} is not HH:MM-HH:MM"))?;
        let (start, end) = (parse_time(a)?, parse_time(b)?);
        ensure!(start != end, "{s:?} is empty");
        ensure!(start < 24 * 60, "{s:?} starts at the end of the day");
        Ok(Self { start, end })
    }
}

impl fmt::Display for HourRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02}:{:02}-{:02}:{:02}",
            self.start / 60,
            self.start % 60,
            self.end / 60,
            self.end % 60
        )
    }
}

/// Parses `22:00-07:00,12:00-13:00`.
pub fn parse_hours(s: &str) -> Result<Vec<HourRange>> {
    let ranges: Vec<HourRange> = s
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::parse)
        .collect::<Result<_>>()?;
    if ranges.is_empty() {
        bail!("no hours given");
    }
    Ok(ranges)
}

struct HourList<'a>(&'a [HourRange]);

impl fmt::Display for HourList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, h) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{h}")?;
        }
        Ok(())
    }
}

fn fmt_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, s % 3600 / 60)
    } else if s >= 60 {
        format!("{}m", s / 60)
    } else {
        format!("{s}s")
    }
}

/// Minutes since local midnight, now.
pub fn local_minute() -> u32 {
    use chrono::Timelike;
    let now = chrono::Local::now();
    now.hour() * 60 + now.minute()
}

/// Reads the machine's state. [`SystemProbe`] is the real one; tests use
/// their own.
pub trait Probe: Send + Sync {
    /// `Some(true)` on battery, `Some(false)` on AC power, `None` unknown
    /// (treated as AC power).
    fn on_battery(&self) -> Option<bool>;
    /// How long since the last keyboard or mouse input, or why that cannot
    /// be read.
    fn idle_time(&self) -> std::result::Result<Duration, String>;
}

/// The policy and the probe it reads, as the agent gets them.
#[derive(Clone)]
pub struct AvailabilityConfig {
    pub policy: Policy,
    pub probe: Arc<dyn Probe>,
    /// How often to check.
    pub interval: Duration,
}

impl AvailabilityConfig {
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            probe: Arc::new(SystemProbe),
            interval: Duration::from_secs(15),
        }
    }

    /// The policy's answer now (runs the probes on a blocking thread).
    pub async fn check(&self) -> Availability {
        let me = self.clone();
        tokio::task::spawn_blocking(move || me.policy.evaluate(&*me.probe, local_minute()))
            .await
            .unwrap_or_else(|e| Availability::Paused {
                reason: format!("availability check failed: {e}"),
            })
    }
}

impl fmt::Debug for AvailabilityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvailabilityConfig")
            .field("policy", &self.policy)
            .field("interval", &self.interval)
            .finish()
    }
}

/// This machine.
pub struct SystemProbe;

impl Probe for SystemProbe {
    fn on_battery(&self) -> Option<bool> {
        #[cfg(target_os = "macos")]
        {
            let out = run("pmset", &["-g", "batt"])?;
            parse_pmset(&out)
        }
        #[cfg(target_os = "linux")]
        {
            linux::on_battery(std::path::Path::new("/sys/class/power_supply"))
        }
        #[cfg(windows)]
        {
            windows::on_battery()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
        None
    }

    fn idle_time(&self) -> std::result::Result<Duration, String> {
        #[cfg(target_os = "macos")]
        {
            let out = run("ioreg", &["-c", "IOHIDSystem", "-d", "4"]).ok_or("ioreg did not run")?;
            parse_hid_idle(&out).ok_or_else(|| "ioreg shows no HIDIdleTime".into())
        }
        #[cfg(target_os = "linux")]
        {
            Ok(linux::idle_time())
        }
        #[cfg(windows)]
        {
            windows::idle_time()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
        Err("not supported on this OS".into())
    }
}

#[cfg(target_os = "macos")]
fn run(program: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `pmset -g batt`: "Now drawing from 'Battery Power'" or 'AC Power' (or
/// 'UPS Power', which counts as AC).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn parse_pmset(out: &str) -> Option<bool> {
    let line = out.lines().find(|l| l.contains("drawing from"))?;
    if line.contains("'Battery Power'") {
        Some(true)
    } else if line.contains("'AC Power'") || line.contains("'UPS Power'") {
        Some(false)
    } else {
        None
    }
}

/// `ioreg -c IOHIDSystem`: `"HIDIdleTime" = 123456789` (nanoseconds).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn parse_hid_idle(out: &str) -> Option<Duration> {
    out.lines().find_map(|l| {
        let rest = l.split("\"HIDIdleTime\" =").nth(1)?;
        let ns: u64 = rest.trim().parse().ok()?;
        Some(Duration::from_nanos(ns))
    })
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        path::Path,
        time::{Duration, SystemTime},
    };

    /// On battery when a battery is discharging and no mains or USB supply
    /// is online. No battery: on AC power.
    pub fn on_battery(dir: &Path) -> Option<bool> {
        let read = |p: &Path, f: &str| {
            std::fs::read_to_string(p.join(f))
                .map(|s| s.trim().to_string())
                .unwrap_or_default()
        };
        let mut discharging = false;
        let mut mains = false;
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let p = entry.path();
            match read(&p, "type").as_str() {
                "Mains" | "USB" if read(&p, "online") == "1" => mains = true,
                "Battery" if read(&p, "status") == "Discharging" => discharging = true,
                _ => {}
            }
        }
        Some(discharging && !mains)
    }

    /// Time since the most recent input anyone gave this machine that we
    /// can see: terminals (local and SSH) and logind sessions. Nobody
    /// logged in at all is idle for as long as the machine has been up.
    pub fn idle_time() -> Duration {
        let now = SystemTime::now();
        let mut idle = Duration::MAX;
        let mut see = |t: SystemTime| {
            idle = idle.min(now.duration_since(t).unwrap_or(Duration::ZERO));
        };
        for dir in ["/dev/pts", "/dev"] {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for e in entries.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                let tty = if dir == "/dev" {
                    // tty1..tty63, the virtual consoles; not tty, ttyS*.
                    name.strip_prefix("tty")
                        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                } else {
                    name.bytes().all(|b| b.is_ascii_digit())
                };
                if tty && let Ok(t) = e.metadata().and_then(|m| m.accessed()) {
                    see(t);
                }
            }
        }
        if let Some(since) = logind_idle_since() {
            see(since);
        }
        idle
    }

    /// The latest time a graphical session stopped being idle, from
    /// logind (`loginctl`). `None` without systemd or without graphical
    /// sessions. A session that is not idle counts as input now.
    fn logind_idle_since() -> Option<SystemTime> {
        let out = std::process::Command::new("loginctl")
            .args(["list-sessions", "--no-legend"])
            .output()
            .ok()?;
        let list = String::from_utf8_lossy(&out.stdout).into_owned();
        let mut latest: Option<SystemTime> = None;
        for id in list.lines().filter_map(|l| l.split_whitespace().next()) {
            let out = std::process::Command::new("loginctl")
                .args([
                    "show-session",
                    id,
                    "-p",
                    "Type",
                    "-p",
                    "Class",
                    "-p",
                    "IdleHint",
                    "-p",
                    "IdleSinceHint",
                ])
                .output()
                .ok()?;
            let text = String::from_utf8_lossy(&out.stdout).into_owned();
            if let Some(t) = session_active_at(&text) {
                latest = Some(latest.map_or(t, |l| l.max(t)));
            }
        }
        latest
    }

    /// For a graphical user session: now if it is not idle, else when it
    /// became idle.
    pub fn session_active_at(show: &str) -> Option<SystemTime> {
        let get = |k: &str| {
            show.lines()
                .find_map(|l| l.strip_prefix(k)?.strip_prefix('='))
                .unwrap_or("")
        };
        if get("Class") != "user" || !matches!(get("Type"), "x11" | "wayland" | "mir") {
            return None;
        }
        if get("IdleHint") != "yes" {
            return Some(SystemTime::now());
        }
        let usec: u64 = get("IdleSinceHint").parse().ok()?;
        Some(SystemTime::UNIX_EPOCH + Duration::from_micros(usec))
    }
}

#[cfg(windows)]
mod windows {
    use std::time::Duration;

    pub fn on_battery() -> Option<bool> {
        use windows_sys::Win32::System::Power::{GetSystemPowerStatus, SYSTEM_POWER_STATUS};
        let mut s: SYSTEM_POWER_STATUS = unsafe { std::mem::zeroed() };
        // SAFETY: fills the struct we pass.
        if unsafe { GetSystemPowerStatus(&mut s) } == 0 {
            return None;
        }
        match s.ACLineStatus {
            0 => Some(true),
            1 => Some(false),
            _ => None,
        }
    }

    pub fn idle_time() -> Result<Duration, String> {
        use windows_sys::Win32::{
            System::{
                RemoteDesktop::ProcessIdToSessionId, SystemInformation::GetTickCount,
                Threading::GetCurrentProcessId,
            },
            UI::Input::KeyboardAndMouse::{GetLastInputInfo, LASTINPUTINFO},
        };
        let mut session = 0u32;
        // SAFETY: plain queries about this process.
        if unsafe { ProcessIdToSessionId(GetCurrentProcessId(), &mut session) } != 0 && session == 0
        {
            return Err("the agent runs as a Windows service, which cannot see the keyboard and mouse; run it in the user's session (`pumasi agent start` at logon) to use --idle-minutes".into());
        }
        let mut info = LASTINPUTINFO {
            cbSize: std::mem::size_of::<LASTINPUTINFO>() as u32,
            dwTime: 0,
        };
        // SAFETY: `info` is initialized with its size.
        if unsafe { GetLastInputInfo(&mut info) } == 0 {
            return Err(format!(
                "GetLastInputInfo failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let now = unsafe { GetTickCount() };
        Ok(Duration::from_millis(now.wrapping_sub(info.dwTime) as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake {
        battery: Option<bool>,
        idle: std::result::Result<Duration, String>,
    }

    impl Probe for Fake {
        fn on_battery(&self) -> Option<bool> {
            self.battery
        }
        fn idle_time(&self) -> std::result::Result<Duration, String> {
            self.idle.clone()
        }
    }

    fn fake(battery: Option<bool>, idle_secs: u64) -> Fake {
        Fake {
            battery,
            idle: Ok(Duration::from_secs(idle_secs)),
        }
    }

    #[test]
    fn hours_parse_and_wrap() {
        let h = parse_hours("22:00-07:00, 12:00-13:30").unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].to_string(), "22:00-07:00");
        assert!(h[0].contains(23 * 60));
        assert!(h[0].contains(0));
        assert!(h[0].contains(6 * 60 + 59));
        assert!(!h[0].contains(7 * 60));
        assert!(h[1].contains(13 * 60 + 29));
        assert!(!h[1].contains(13 * 60 + 30));
        let day = parse_hours("00:00-24:00").unwrap();
        assert!(day[0].contains(0) && day[0].contains(24 * 60 - 1));
        for bad in [
            "",
            "9-17",
            "25:00-01:00",
            "10:00-10:00",
            "10:60-11:00",
            "24:00-01:00",
        ] {
            assert!(parse_hours(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn policy_pauses_for_each_condition() {
        let p = Policy {
            only_on_ac: true,
            idle_for: Some(Duration::from_secs(600)),
            hours: parse_hours("09:00-18:00").unwrap(),
        };
        let noon = 12 * 60;
        assert!(p.evaluate(&fake(Some(false), 900), noon).is_available());
        // No battery information: treated as AC power.
        assert!(p.evaluate(&fake(None, 900), noon).is_available());
        let r = p.evaluate(&fake(Some(true), 900), noon);
        assert_eq!(r.paused_reason(), Some("on battery power"));
        let r = p.evaluate(&fake(Some(false), 30), noon);
        assert_eq!(
            r.paused_reason(),
            Some("in use (idle 30s of the 10m required)")
        );
        let r = p.evaluate(&fake(Some(false), 900), 20 * 60);
        assert_eq!(r.paused_reason(), Some("outside its hours (09:00-18:00)"));
        let unknown = Fake {
            battery: Some(false),
            idle: Err("no way to tell".into()),
        };
        let r = p.evaluate(&unknown, noon);
        assert!(r.paused_reason().unwrap().contains("no way to tell"));
        // No policy: always available, nothing probed.
        assert!(!Policy::default().is_active());
        assert!(Policy::default().evaluate(&unknown, 0).is_available());
    }

    #[test]
    fn parses_macos_tools() {
        let ac = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1234)\t100%; charged; 0:00 remaining present: true\n";
        let batt = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1234)\t87%; discharging; 4:12 remaining present: true\n";
        assert_eq!(parse_pmset(ac), Some(false));
        assert_eq!(parse_pmset(batt), Some(true));
        assert_eq!(parse_pmset("Now drawing from 'UPS Power'"), Some(false));
        assert_eq!(parse_pmset("garbage"), None);
        let ioreg = "  | |   \"HIDIdleTime\" = 61234567890\n  | |   \"HIDKeyboardModifierMappingPairs\" = ()\n";
        assert_eq!(
            parse_hid_idle(ioreg),
            Some(Duration::from_nanos(61234567890))
        );
        assert_eq!(parse_hid_idle("nothing"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_power_supplies() {
        let dir = crate::tempdir::TempDir::new("power").unwrap();
        let supply = |name: &str, files: &[(&str, &str)]| {
            let d = dir.path().join(name);
            std::fs::create_dir_all(&d).unwrap();
            for (f, v) in files {
                std::fs::write(d.join(f), format!("{v}\n")).unwrap();
            }
        };
        // A desktop: nothing at all.
        assert_eq!(linux::on_battery(dir.path()), Some(false));
        supply("BAT0", &[("type", "Battery"), ("status", "Discharging")]);
        assert_eq!(linux::on_battery(dir.path()), Some(true));
        supply("AC", &[("type", "Mains"), ("online", "1")]);
        assert_eq!(linux::on_battery(dir.path()), Some(false));
        supply("AC", &[("type", "Mains"), ("online", "0")]);
        assert_eq!(linux::on_battery(dir.path()), Some(true));
        // Reading the real machine never fails.
        let _ = SystemProbe.on_battery();
        assert!(SystemProbe.idle_time().is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn logind_sessions() {
        let tty = "Type=tty\nClass=user\nIdleHint=no\nIdleSinceHint=0\n";
        assert!(linux::session_active_at(tty).is_none());
        let busy = "Type=wayland\nClass=user\nIdleHint=no\nIdleSinceHint=0\n";
        let t = linux::session_active_at(busy).unwrap();
        assert!(t.elapsed().unwrap_or_default() < Duration::from_secs(5));
        let idle = "Type=x11\nClass=user\nIdleHint=yes\nIdleSinceHint=1700000000000000\n";
        assert_eq!(
            linux::session_active_at(idle),
            Some(std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
        );
        let greeter = "Type=x11\nClass=greeter\nIdleHint=no\n";
        assert!(linux::session_active_at(greeter).is_none());
    }
}
