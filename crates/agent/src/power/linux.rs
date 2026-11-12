//! Linux: logind inhibitors and its `PrepareForSleep` signal, through the
//! tools every systemd machine has (`systemd-inhibit`, and one of
//! `gdbus`, `dbus-monitor` or `busctl` to follow the signal).

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

use super::{PowerBackend, PowerEvent, PowerHandler};

pub struct Logind;

/// A `systemd-inhibit ... sleep infinity` child: the lock is held while
/// it lives.
struct Inhibitor(Child);

impl Drop for Inhibitor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn inhibit(mode: &str, why: &str) -> Result<Inhibitor, String> {
    let mut child = Command::new("systemd-inhibit")
        .args([
            "--what=sleep",
            "--who=pumasi",
            &format!("--why={why}"),
            &format!("--mode={mode}"),
            "sleep",
            "infinity",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("systemd-inhibit did not start: {e}"))?;
    // It exits at once when logind refuses (no systemd, no permission).
    std::thread::sleep(Duration::from_millis(250));
    match child.try_wait() {
        Ok(None) => Ok(Inhibitor(child)),
        Ok(Some(status)) => Err(format!(
            "logind refused the sleep inhibitor (systemd-inhibit: {status})"
        )),
        Err(e) => Err(format!("systemd-inhibit: {e}")),
    }
}

const MATCH: &str =
    "type='signal',interface='org.freedesktop.login1.Manager',member='PrepareForSleep'";

/// Starts whichever tool can follow logind's signals.
fn monitor() -> Result<Child, String> {
    let tools: [(&str, Vec<&str>); 3] = [
        (
            "gdbus",
            vec![
                "monitor",
                "--system",
                "--dest",
                "org.freedesktop.login1",
                "--object-path",
                "/org/freedesktop/login1",
            ],
        ),
        ("dbus-monitor", vec!["--system", MATCH]),
        ("busctl", vec!["monitor", "--system", "--match", MATCH]),
    ];
    for (tool, args) in tools {
        let child = Command::new(tool)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn();
        let Ok(mut child) = child else {
            continue;
        };
        std::thread::sleep(Duration::from_millis(250));
        if matches!(child.try_wait(), Ok(None)) {
            return Ok(child);
        }
    }
    Err("none of gdbus, dbus-monitor and busctl can follow logind's PrepareForSleep signal".into())
}

/// Reads the tools' output: `Some(true)` when a line (or the line after
/// one that names the signal) says the machine is about to sleep,
/// `Some(false)` when it says it woke.
pub(super) struct SignalParser {
    pending: bool,
}

impl SignalParser {
    pub(super) fn new() -> Self {
        Self { pending: false }
    }

    pub(super) fn line(&mut self, line: &str) -> Option<bool> {
        let lower = line.to_ascii_lowercase();
        let named = line.contains("PrepareForSleep");
        if !named && !self.pending {
            return None;
        }
        // The value: on the signal's own line (gdbus) or on a later one
        // (dbus-monitor, busctl).
        let rest = match line.find("PrepareForSleep") {
            Some(i) => &lower[i + "PrepareForSleep".len()..],
            None => &lower[..],
        };
        let value = if rest.contains("true") {
            Some(true)
        } else if rest.contains("false") {
            Some(false)
        } else {
            None
        };
        self.pending = value.is_none();
        value
    }
}

impl PowerBackend for Logind {
    fn stay_awake(&self) -> Result<Box<dyn Send>, String> {
        Ok(Box::new(inhibit("block", "running cluster tasks")?))
    }

    fn watch(&self, on_event: PowerHandler) -> Result<(), String> {
        let why = "telling the cluster before sleep";
        // The delay lock gives the agent its moment before sleep.
        let mut delay = Some(inhibit("delay", why)?);
        let mut child = monitor()?;
        let out = child.stdout.take().ok_or("no output from the monitor")?;
        std::thread::Builder::new()
            .name("pumasi-power".into())
            .spawn(move || {
                let mut parser = SignalParser::new();
                for line in BufReader::new(out).lines() {
                    let Ok(line) = line else { break };
                    match parser.line(&line) {
                        Some(true) => {
                            on_event(PowerEvent::Sleep);
                            // Letting go of the lock lets logind sleep.
                            delay = None;
                        }
                        Some(false) => {
                            on_event(PowerEvent::Wake);
                            delay = inhibit("delay", why).ok();
                        }
                        None => {}
                    }
                }
                let _ = child.kill();
                let _ = child.wait();
                drop(delay);
            })
            .map_err(|e| format!("thread: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SignalParser;

    #[test]
    fn reads_the_sleep_signal_from_each_tool() {
        // gdbus monitor
        let mut p = SignalParser::new();
        assert_eq!(p.line("Monitoring signals on object /org/freedesktop/login1 owned by org.freedesktop.login1"), None);
        assert_eq!(
            p.line(
                "/org/freedesktop/login1: org.freedesktop.login1.Manager.PrepareForSleep (true,)"
            ),
            Some(true)
        );
        assert_eq!(
            p.line("/org/freedesktop/login1: org.freedesktop.login1.Manager.SessionNew ('3', objectpath '/x')"),
            None
        );
        assert_eq!(
            p.line(
                "/org/freedesktop/login1: org.freedesktop.login1.Manager.PrepareForSleep (false,)"
            ),
            Some(false)
        );
        // dbus-monitor
        let mut p = SignalParser::new();
        assert_eq!(
            p.line("signal time=1.5 sender=:1.4 -> destination=(null destination) serial=9 path=/org/freedesktop/login1; interface=org.freedesktop.login1.Manager; member=PrepareForSleep"),
            None
        );
        assert_eq!(p.line("   boolean true"), Some(true));
        assert_eq!(p.line("   boolean true"), None);
        // busctl monitor
        let mut p = SignalParser::new();
        assert_eq!(p.line("  Member=PrepareForSleep"), None);
        assert_eq!(p.line("  MESSAGE \"b\" {"), None);
        assert_eq!(p.line("          BOOLEAN false;"), Some(false));
    }
}
