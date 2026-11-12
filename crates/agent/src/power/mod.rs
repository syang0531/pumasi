//! Machines that sleep (1.5): the agent keeps its machine awake while it
//! runs tasks, tells the control plane before the machine sleeps, and
//! sends other nodes' Wake-on-LAN packets when asked.
//!
//! | | Stay awake | Notice of sleep |
//! | --- | --- | --- |
//! | macOS | an IOKit power assertion (`PreventUserIdleSystemSleep`, as `caffeinate -i`) | `IORegisterForSystemPower` |
//! | Windows | a power request (`PowerSetRequest`, `PowerRequestSystemRequired`; shown by `powercfg /requests`) | `PowerRegisterSuspendResumeNotification` (`PBT_APMSUSPEND`) |
//! | Linux | a logind `sleep` inhibitor in block mode (`systemd-inhibit`) | logind's `PrepareForSleep` signal, behind a delay inhibitor |
//!
//! The blocker is held from the first running task until a little after
//! the last one ends, so the display still sleeps and the machine sleeps
//! on its normal schedule afterwards. It stops idle sleep only: closing a
//! lid or choosing Sleep still sleeps the machine, and then the agent says
//! goodbye first, within the few seconds the OS gives it.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use pumasi_proto::power::{OnPause, PowerReport, WakeSender};
use tokio::sync::watch;
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

/// How long the OS is held back while the agent tells the control plane
/// that the machine sleeps.
const GOODBYE_TIMEOUT: Duration = Duration::from_millis(2500);
/// How long the blocker stays after the last task: the next queued task
/// usually arrives within this.
pub const DEFAULT_LINGER: Duration = Duration::from_secs(30);

/// What the OS says about the machine's sleep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerEvent {
    /// The machine is about to sleep. The backend lets the OS go on when
    /// the handler returns.
    Sleep,
    /// It woke up, or did not sleep after all.
    Wake,
}

pub type PowerHandler = Arc<dyn Fn(PowerEvent) + Send + Sync>;

/// The OS's sleep blocker and sleep notices. [`system`] is the real one;
/// tests use their own.
pub trait PowerBackend: Send + Sync {
    /// Keeps the machine from sleeping on its own until the returned
    /// value is dropped.
    fn stay_awake(&self) -> Result<Box<dyn Send>, String>;
    /// Calls `on_event` (from a thread of the backend's own) whenever the
    /// machine is about to sleep or has woken.
    fn watch(&self, on_event: PowerHandler) -> Result<(), String>;
}

/// This machine's power backend.
pub fn system() -> Arc<dyn PowerBackend> {
    #[cfg(target_os = "linux")]
    {
        Arc::new(linux::Logind)
    }
    #[cfg(target_os = "macos")]
    {
        Arc::new(macos::IoKit)
    }
    #[cfg(windows)]
    {
        Arc::new(windows::WinPower)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        Arc::new(Unsupported)
    }
}

/// An OS with no backend.
#[cfg_attr(
    any(target_os = "linux", target_os = "macos", windows),
    allow(dead_code)
)]
struct Unsupported;

impl PowerBackend for Unsupported {
    fn stay_awake(&self) -> Result<Box<dyn Send>, String> {
        Err("not supported on this OS".into())
    }
    fn watch(&self, _: PowerHandler) -> Result<(), String> {
        Err("not supported on this OS".into())
    }
}

/// What the agent does about its machine's sleep.
#[derive(Clone, Default)]
pub struct PowerOptions {
    /// The OS backend. `None`: the agent neither keeps the machine awake
    /// nor notices its sleep.
    pub backend: Option<Arc<dyn PowerBackend>>,
    /// Do not keep the machine awake while tasks run (`--allow-sleep`).
    pub allow_sleep: bool,
    /// Let the cluster wake this machine over the LAN (`--wake-on-lan`).
    pub wake_on_lan: bool,
    pub on_pause: OnPause,
    /// Lists this machine's LAN interfaces and sends magic packets.
    /// `None`: the agent reports no interfaces and sends none.
    pub wake: Option<Arc<dyn WakeSender>>,
    /// How long the blocker stays after the last task ends.
    pub linger: Duration,
}

impl PowerOptions {
    /// The real backends, with the owner's choices.
    pub fn system(allow_sleep: bool, wake_on_lan: bool, on_pause: OnPause) -> Self {
        Self {
            backend: Some(system()),
            allow_sleep,
            wake_on_lan,
            on_pause,
            wake: Some(Arc::new(pumasi_proto::power::SystemWake)),
            linger: DEFAULT_LINGER,
        }
    }
}

impl std::fmt::Debug for PowerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowerOptions")
            .field("backend", &self.backend.is_some())
            .field("allow_sleep", &self.allow_sleep)
            .field("wake_on_lan", &self.wake_on_lan)
            .field("on_pause", &self.on_pause)
            .finish()
    }
}

#[derive(Default)]
struct Holds {
    /// Tasks running now.
    tasks: u32,
    /// Bumped whenever a task starts, so a release scheduled before it
    /// does nothing.
    epoch: u64,
    blocker: Option<Box<dyn Send>>,
    /// The blocker could not be taken: said once.
    failed: bool,
}

/// The agent's side of its machine's sleep.
pub struct Power {
    opts: PowerOptions,
    holds: Arc<Mutex<Holds>>,
    asleep: watch::Receiver<bool>,
    /// Set while a session wants to say goodbye before the machine sleeps.
    in_session: Arc<AtomicBool>,
    /// Dropped once the goodbye is done, which lets the OS go on.
    goodbye: Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>>,
    sleep_notice: bool,
    notes: Vec<String>,
}

impl Power {
    pub fn new(opts: PowerOptions) -> Arc<Self> {
        let (asleep_tx, asleep) = watch::channel(false);
        let in_session = Arc::new(AtomicBool::new(false));
        let goodbye: Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>> = Default::default();
        let mut notes = Vec::new();
        let mut sleep_notice = false;
        if let Some(backend) = &opts.backend {
            let (in_session, goodbye) = (in_session.clone(), goodbye.clone());
            let handler: PowerHandler = Arc::new(move |ev| match ev {
                PowerEvent::Sleep => {
                    info!("the machine is going to sleep");
                    let (tx, rx) = std::sync::mpsc::channel::<()>();
                    if in_session.load(Ordering::SeqCst) {
                        *goodbye.lock().unwrap() = Some(tx);
                    } else {
                        drop(tx);
                    }
                    asleep_tx.send_replace(true);
                    // Until the session said goodbye (it drops the sender).
                    let _ = rx.recv_timeout(GOODBYE_TIMEOUT);
                    goodbye.lock().unwrap().take();
                }
                PowerEvent::Wake => {
                    info!("the machine is awake");
                    asleep_tx.send_replace(false);
                }
            });
            match backend.watch(handler) {
                Ok(()) => sleep_notice = true,
                Err(why) => {
                    warn!("cannot tell the cluster before this machine sleeps: {why}");
                    notes.push(format!("no notice of sleep: {why}"));
                }
            }
        }
        Arc::new(Self {
            opts,
            holds: Default::default(),
            asleep,
            in_session,
            goodbye,
            sleep_notice,
            notes,
        })
    }

    /// Call when a task starts; keeps the machine awake until the
    /// returned value is dropped (and a little longer).
    pub fn task(self: &Arc<Self>) -> TaskAwake {
        let mut h = self.holds.lock().unwrap();
        h.tasks += 1;
        h.epoch += 1;
        if h.blocker.is_none()
            && !self.opts.allow_sleep
            && let Some(backend) = &self.opts.backend
        {
            match backend.stay_awake() {
                Ok(b) => {
                    debug!("keeping the machine awake while tasks run");
                    h.blocker = Some(b);
                    h.failed = false;
                }
                Err(why) => {
                    if !h.failed {
                        warn!("cannot keep this machine awake while tasks run: {why}");
                    }
                    h.failed = true;
                }
            }
        }
        TaskAwake(self.clone())
    }

    fn task_done(&self) {
        let epoch = {
            let mut h = self.holds.lock().unwrap();
            h.tasks = h.tasks.saturating_sub(1);
            if h.tasks > 0 || h.blocker.is_none() {
                return;
            }
            h.epoch
        };
        let holds = self.holds.clone();
        let release = move || {
            let mut h = holds.lock().unwrap();
            if h.tasks == 0 && h.epoch == epoch && h.blocker.take().is_some() {
                debug!("idle: the machine may sleep again");
            }
        };
        let linger = self.opts.linger;
        match tokio::runtime::Handle::try_current() {
            Ok(rt) if !linger.is_zero() => {
                rt.spawn(async move {
                    tokio::time::sleep(linger).await;
                    release();
                });
            }
            _ => release(),
        }
    }

    /// Whether the blocker is held now.
    pub fn holding(&self) -> bool {
        self.holds.lock().unwrap().blocker.is_some()
    }

    /// Follows whether the machine is going to sleep (or sleeps).
    pub fn asleep(&self) -> watch::Receiver<bool> {
        self.asleep.clone()
    }

    /// A session is up: a sleep notice waits for its goodbye.
    pub fn session(&self, up: bool) {
        self.in_session.store(up, Ordering::SeqCst);
        if !up {
            self.goodbye_done();
        }
    }

    /// The control plane knows (or cannot be told): the OS may go on.
    pub fn goodbye_done(&self) {
        self.goodbye.lock().unwrap().take();
    }

    /// Waits until the machine is awake again, at most `max`: a wake
    /// notice that never comes must not keep the agent away for ever.
    pub async fn wait_awake(&self, max: Duration) {
        let mut asleep = self.asleep.clone();
        let _ = tokio::time::timeout(max, asleep.wait_for(|a| !*a)).await;
    }

    pub fn wake_sender(&self) -> Option<Arc<dyn WakeSender>> {
        self.opts.wake.clone()
    }

    /// What the control plane is told (reads the LAN interfaces).
    pub fn report(&self) -> PowerReport {
        let mut notes = self.notes.clone();
        let stays_awake = !self.opts.allow_sleep && self.opts.backend.is_some() && {
            let h = self.holds.lock().unwrap();
            !h.failed
        };
        if self.holds.lock().unwrap().failed {
            notes.push("cannot keep the machine awake (see the agent's log)".into());
        }
        PowerReport {
            stays_awake,
            sleep_notice: self.sleep_notice,
            wake_on_lan: self.opts.wake_on_lan,
            lan: self.opts.wake.as_ref().map(|w| w.lan()).unwrap_or_default(),
            on_pause: self.opts.on_pause,
            notes,
        }
    }
}

/// A running task's hold on the machine staying awake.
pub struct TaskAwake(Arc<Power>);

impl Drop for TaskAwake {
    fn drop(&mut self) {
        self.0.task_done();
    }
}

/// A stand-in backend for tests: counts blockers and lets the test play
/// the OS.
#[derive(Default)]
pub struct FakePower {
    held: Arc<std::sync::atomic::AtomicU32>,
    handler: Mutex<Option<PowerHandler>>,
}

impl FakePower {
    /// Blockers held now.
    pub fn held(&self) -> u32 {
        self.held.load(Ordering::SeqCst)
    }

    /// Plays the OS announcing sleep (blocks like the OS does, until the
    /// agent is done or the time it gets is over) or waking.
    pub fn announce(&self, ev: PowerEvent) {
        let handler = self.handler.lock().unwrap().clone();
        if let Some(h) = handler {
            h(ev);
        }
    }
}

struct FakeHold(Arc<std::sync::atomic::AtomicU32>);

impl Drop for FakeHold {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl PowerBackend for FakePower {
    fn stay_awake(&self) -> Result<Box<dyn Send>, String> {
        self.held.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeHold(self.held.clone())))
    }

    fn watch(&self, on_event: PowerHandler) -> Result<(), String> {
        *self.handler.lock().unwrap() = Some(on_event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn power(fake: &Arc<FakePower>, allow_sleep: bool, linger: Duration) -> Arc<Power> {
        Power::new(PowerOptions {
            backend: Some(fake.clone()),
            allow_sleep,
            linger,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn the_blocker_is_held_while_tasks_run_and_a_little_longer() {
        let fake = Arc::new(FakePower::default());
        let p = power(&fake, false, Duration::from_millis(80));
        assert_eq!(fake.held(), 0);
        let a = p.task();
        let b = p.task();
        assert_eq!(fake.held(), 1, "one blocker for any number of tasks");
        drop(a);
        assert_eq!(fake.held(), 1);
        drop(b);
        // Lingers, and a task that starts meanwhile keeps it.
        assert_eq!(fake.held(), 1);
        let c = p.task();
        tokio::time::sleep(Duration::from_millis(160)).await;
        assert_eq!(fake.held(), 1, "released under a running task");
        drop(c);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(fake.held(), 0);
        assert!(p.report().stays_awake && p.report().sleep_notice);

        // --allow-sleep: never held.
        let p = power(&fake, true, Duration::ZERO);
        let t = p.task();
        assert_eq!(fake.held(), 0);
        drop(t);
        assert!(!p.report().stays_awake);
    }

    #[tokio::test]
    async fn the_os_waits_for_the_goodbye_but_not_for_ever() {
        let fake = Arc::new(FakePower::default());
        let p = power(&fake, false, Duration::ZERO);
        let mut asleep = p.asleep();
        // No session: nothing to wait for.
        let f = fake.clone();
        let start = std::time::Instant::now();
        tokio::task::spawn_blocking(move || f.announce(PowerEvent::Sleep))
            .await
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(*asleep.borrow_and_update());
        fake.announce(PowerEvent::Wake);
        assert!(!*asleep.borrow_and_update());

        // In a session the OS is held until the goodbye is done.
        p.session(true);
        let f = fake.clone();
        let os = tokio::task::spawn_blocking(move || f.announce(PowerEvent::Sleep));
        asleep.changed().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!os.is_finished(), "the OS went on before the goodbye");
        p.goodbye_done();
        tokio::time::timeout(Duration::from_secs(1), os)
            .await
            .expect("the OS was not let go")
            .unwrap();
        p.wait_awake(Duration::from_millis(50)).await;
    }
}
