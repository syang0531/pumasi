//! The Windows half of [`super`]: `pumasi docker setup`, `status` and
//! `remove` (administrator), the task's `__wsl-docker-run` (as
//! [`ACCOUNT`]), and the agent's [`kick`].

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

use super::*;

/// How long `setup` waits for the first start (downloads and apt-get).
const FIRST_START: Duration = Duration::from_secs(30 * 60);
/// The log is cut when it reaches this size (one older file kept).
const LOG_LIMIT: u64 = 8 << 20;
/// SIDs of LocalSystem and the Administrators group, as `icacls` takes them.
const SYSTEM_SID: &str = "*S-1-5-18";
const ADMINS_SID: &str = "*S-1-5-32-544";
const USERS_SID: &str = "*S-1-5-32-545";
const USER_LIST: &str =
    r"HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon\SpecialAccounts\UserList";

/// `%SystemRoot%\System32\<exe>`, so a service's or task's PATH does not
/// matter.
fn system32(exe: &str) -> PathBuf {
    let root = std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into());
    PathBuf::from(root).join("System32").join(exe)
}

fn wsl() -> Command {
    let mut cmd = Command::new(system32("wsl.exe"));
    cmd.env("WSL_UTF8", "1").stdin(Stdio::null());
    cmd
}

/// Runs a command to its end within `limit`.
async fn output(mut cmd: Command, limit: Duration) -> Result<std::process::Output> {
    let program = format!("{:?}", cmd.as_std().get_program());
    cmd.stdin(Stdio::null()).kill_on_drop(true);
    tokio::time::timeout(limit, cmd.output())
        .await
        .with_context(|| format!("{program} did not finish within {} s", limit.as_secs()))?
        .with_context(|| format!("run {program}"))
}

/// Runs a system tool, failing with its output when it fails.
async fn tool(exe: &str, args: &[&str]) -> Result<String> {
    let mut cmd = Command::new(system32(exe));
    cmd.args(args);
    let out = output(cmd, Duration::from_secs(120)).await?;
    let text = format!(
        "{}{}",
        wsl_text(&out.stdout).trim(),
        wsl_text(&out.stderr).trim()
    );
    ensure!(
        out.status.success(),
        "{exe} {} failed: {text}",
        args.join(" ")
    );
    Ok(text)
}

fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Whether this process runs elevated (an administrator terminal).
fn elevated() -> bool {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE},
        Security::{GetTokenInformation, TOKEN_ELEVATION, TOKEN_QUERY, TokenElevation},
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: plain Win32 calls on this process's own token, closed below.
    unsafe {
        if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
            return false;
        }
        let mut e = TOKEN_ELEVATION { TokenIsElevated: 0 };
        let mut len = 0u32;
        let ok = GetTokenInformation(
            token,
            TokenElevation,
            &mut e as *mut _ as *mut core::ffi::c_void,
            std::mem::size_of::<TOKEN_ELEVATION>() as u32,
            &mut len,
        );
        CloseHandle(token);
        ok != 0 && e.TokenIsElevated != 0
    }
}

/// Creates [`ACCOUNT`] (a standard user whose password never expires), or
/// gives it `password` when it exists. True if it was created.
fn ensure_account(password: &str) -> Result<bool> {
    use windows_sys::Win32::NetworkManagement::NetManagement::{
        NERR_UserExists, NetUserAdd, NetUserSetInfo, UF_DONT_EXPIRE_PASSWD, UF_PASSWD_CANT_CHANGE,
        UF_SCRIPT, USER_INFO_1, USER_INFO_1003, USER_PRIV_USER,
    };
    let mut name = wide(ACCOUNT);
    let mut pw = wide(password);
    let mut comment = wide("Runs the Pumasi Docker WSL distro (pumasi docker setup)");
    let info = USER_INFO_1 {
        usri1_name: name.as_mut_ptr(),
        usri1_password: pw.as_mut_ptr(),
        usri1_password_age: 0,
        usri1_priv: USER_PRIV_USER,
        usri1_home_dir: std::ptr::null_mut(),
        usri1_comment: comment.as_mut_ptr(),
        usri1_flags: UF_SCRIPT | UF_DONT_EXPIRE_PASSWD | UF_PASSWD_CANT_CHANGE,
        usri1_script_path: std::ptr::null_mut(),
    };
    let mut parm = 0u32;
    // SAFETY: `info` points at NUL-terminated buffers that outlive the call.
    let rc = unsafe {
        NetUserAdd(
            std::ptr::null(),
            1,
            &info as *const _ as *const u8,
            &mut parm,
        )
    };
    let created = match rc {
        0 => true,
        rc if rc == NERR_UserExists => {
            let info = USER_INFO_1003 {
                usri1003_password: pw.as_mut_ptr(),
            };
            // SAFETY: as above.
            let rc = unsafe {
                NetUserSetInfo(
                    std::ptr::null(),
                    name.as_ptr(),
                    1003,
                    &info as *const _ as *const u8,
                    &mut parm,
                )
            };
            ensure!(
                rc == 0,
                "could not set the password of {ACCOUNT} (error {rc})"
            );
            false
        }
        rc => bail!(
            "could not create the local account {ACCOUNT} (error {rc}{})",
            if rc == 2245 {
                ": the password policy refused the generated password"
            } else {
                ""
            }
        ),
    };
    pw.fill(0);
    Ok(created)
}

/// The SID of [`ACCOUNT`], when it exists.
fn account_sid() -> Option<Vec<u8>> {
    use windows_sys::Win32::Security::{LookupAccountNameW, SID_NAME_USE};
    let name = wide(ACCOUNT);
    let (mut sid_len, mut dom_len, mut kind) = (0u32, 0u32, 0 as SID_NAME_USE);
    // SAFETY: the first call only asks for the sizes.
    unsafe {
        LookupAccountNameW(
            std::ptr::null(),
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut sid_len,
            std::ptr::null_mut(),
            &mut dom_len,
            &mut kind,
        )
    };
    if sid_len == 0 {
        return None;
    }
    let mut sid = vec![0u8; sid_len as usize];
    let mut dom = vec![0u16; dom_len.max(1) as usize];
    // SAFETY: buffers of the sizes the first call gave.
    let ok = unsafe {
        LookupAccountNameW(
            std::ptr::null(),
            name.as_ptr(),
            sid.as_mut_ptr().cast(),
            &mut sid_len,
            dom.as_mut_ptr(),
            &mut dom_len,
            &mut kind,
        )
    };
    (ok != 0).then_some(sid)
}

/// Gives [`ACCOUNT`] "Log on as a batch job", which a task that logs on
/// with a stored password needs (Task Scheduler does not start it
/// otherwise), or takes it back.
fn batch_logon(grant: bool) -> Result<()> {
    use windows_sys::Win32::Security::Authentication::Identity::{
        LSA_HANDLE, LSA_OBJECT_ATTRIBUTES, LSA_UNICODE_STRING, LsaAddAccountRights, LsaClose,
        LsaNtStatusToWinError, LsaOpenPolicy, LsaRemoveAccountRights, POLICY_CREATE_ACCOUNT,
        POLICY_LOOKUP_NAMES,
    };
    let Some(mut sid) = account_sid() else {
        ensure!(!grant, "the local account {ACCOUNT} is missing");
        return Ok(());
    };
    let attrs = LSA_OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<LSA_OBJECT_ATTRIBUTES>() as u32,
        ..Default::default()
    };
    let mut policy: LSA_HANDLE = 0;
    // SAFETY: zeroed attributes, as LsaOpenPolicy wants them.
    let status = unsafe {
        LsaOpenPolicy(
            std::ptr::null(),
            &attrs,
            (POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES) as u32,
            &mut policy,
        )
    };
    // SAFETY: plain conversion.
    let err = |status| unsafe { LsaNtStatusToWinError(status) };
    ensure!(
        status == 0,
        "could not open the local security policy (error {})",
        err(status)
    );
    let mut right = "SeBatchLogonRight".encode_utf16().collect::<Vec<u16>>();
    let bytes = (right.len() * 2) as u16;
    let rights = LSA_UNICODE_STRING {
        Length: bytes,
        MaximumLength: bytes,
        Buffer: right.as_mut_ptr(),
    };
    // SAFETY: `sid` came from LookupAccountNameW, `rights` points at
    // `right`; both outlive the calls.
    let status = unsafe {
        let status = if grant {
            LsaAddAccountRights(policy, sid.as_mut_ptr().cast(), &rights, 1)
        } else {
            LsaRemoveAccountRights(policy, sid.as_mut_ptr().cast(), false, &rights, 1)
        };
        LsaClose(policy);
        status
    };
    // ERROR_FILE_NOT_FOUND: the account held no rights to take back.
    let code = err(status);
    ensure!(
        status == 0 || (!grant && code == 2),
        "could not {} \"Log on as a batch job\" for {ACCOUNT} (error {code})",
        if grant { "grant" } else { "revoke" }
    );
    Ok(())
}

fn delete_account() -> Result<bool> {
    use windows_sys::Win32::NetworkManagement::NetManagement::{NERR_UserNotFound, NetUserDel};
    let name = wide(ACCOUNT);
    // SAFETY: a NUL-terminated name.
    match unsafe { NetUserDel(std::ptr::null(), name.as_ptr()) } {
        0 => Ok(true),
        rc if rc == NERR_UserNotFound => Ok(false),
        rc => bail!("could not delete the local account {ACCOUNT} (error {rc})"),
    }
}

/// Gives the directories their ACLs: everything readable by SYSTEM,
/// administrators and the account only; the account may change just the
/// distro's disk and the logs; the client key is SYSTEM's and the
/// administrators'. The `Pumasi` parent is the administrators', so no user
/// who made it first can swap what is inside.
async fn restrict(layout: &Layout) -> Result<()> {
    let parent = layout
        .root()
        .parent()
        .context("the Pumasi Docker directory has no parent")?;
    let p = parent.display().to_string();
    let root = layout.root().display().to_string();
    let full = |sid: &str| format!("{sid}:(OI)(CI)F");
    tool("icacls.exe", &[&p, "/setowner", ADMINS_SID, "/Q"]).await?;
    tool(
        "icacls.exe",
        &[
            &p,
            "/inheritance:r",
            "/grant:r",
            &full(SYSTEM_SID),
            &full(ADMINS_SID),
            &format!("{USERS_SID}:(OI)(CI)RX"),
            "/Q",
        ],
    )
    .await?;
    tool(
        "icacls.exe",
        &[
            &root,
            "/inheritance:r",
            "/grant:r",
            &full(SYSTEM_SID),
            &full(ADMINS_SID),
            &format!("{ACCOUNT}:(OI)(CI)RX"),
            "/Q",
        ],
    )
    .await?;
    for dir in [layout.distro_dir(), layout.logs()] {
        tool(
            "icacls.exe",
            &[
                &dir.display().to_string(),
                "/grant:r",
                &format!("{ACCOUNT}:(OI)(CI)M"),
                "/Q",
            ],
        )
        .await?;
    }
    tool(
        "icacls.exe",
        &[
            &layout.client_tls().display().to_string(),
            "/inheritance:r",
            "/grant:r",
            &full(SYSTEM_SID),
            &full(ADMINS_SID),
            "/Q",
        ],
    )
    .await?;
    Ok(())
}

/// Copies a pinned file into `downloads` (kept for the next run).
async fn download(layout: &Layout, pin: &Pin) -> Result<PathBuf> {
    let dest = layout.downloads().join(pin.file);
    if dest.is_file() && std::fs::metadata(&dest).map(|m| m.len()).ok() == Some(pin.size) {
        return Ok(dest);
    }
    println!("downloading {} ({} MiB)...", pin.label, pin.size >> 20);
    let http = pumasi_proto::registry::http_client()?;
    crate::microvm::setup::download_to(&http, pin.urls, pin.sha256, pin.size, &dest).await?;
    Ok(dest)
}

/// Takes `docker.exe` out of Docker's zip with Windows' own `tar`.
async fn extract_cli(zip: &Path, layout: &Layout) -> Result<()> {
    let tmp = layout.downloads().join("docker-cli");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    tool(
        "tar.exe",
        &[
            "-xf",
            &zip.display().to_string(),
            "-C",
            &tmp.display().to_string(),
            "docker/docker.exe",
        ],
    )
    .await?;
    let exe = layout.docker_exe();
    let staged = tmp.join("docker").join("docker.exe");
    let _ = std::fs::remove_file(&exe);
    std::fs::rename(&staged, &exe)
        .or_else(|_| std::fs::copy(&staged, &exe).map(|_| ()))
        .with_context(|| format!("install {}", exe.display()))?;
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(())
}

/// What `wsl.exe --version` says, or why WSL is missing.
async fn wsl_version() -> Result<String> {
    let mut cmd = wsl();
    cmd.arg("--version");
    let out = output(cmd, Duration::from_secs(60)).await?;
    let text = wsl_text(&out.stdout);
    ensure!(
        out.status.success() && text.contains('2'),
        "{}",
        wsl_text(&out.stderr).trim()
    );
    Ok(text.lines().next().unwrap_or_default().trim().to_string())
}

async fn schtasks(args: &[&str]) -> Result<String> {
    tool("schtasks.exe", args).await
}

/// The task's state as Task Scheduler names it (`Running`, `Ready`, ...).
async fn task_state() -> Option<String> {
    let (path, name) = TASK.rsplit_once('\\')?;
    let script = format!(
        "$t = Get-ScheduledTask -TaskPath '{path}\\' -TaskName '{name}' -ErrorAction Stop; \
         $i = $t | Get-ScheduledTaskInfo; \
         \"$($t.State) (last run $($i.LastRunTime), result 0x$('{{0:X}}' -f $i.LastTaskResult))\""
    );
    let mut cmd = Command::new(system32(r"WindowsPowerShell\v1.0\powershell.exe"));
    cmd.args(["-NoProfile", "-NonInteractive", "-Command", &script]);
    let out = output(cmd, Duration::from_secs(60)).await.ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Whether the daemon answers: its version.
async fn docker_version(installed: &Installed) -> Result<String> {
    let mut cmd = docker_command(&installed.layout.docker_exe().display().to_string());
    cmd.args(["version", "--format", "{{.Server.Version}}"]);
    let out = output(cmd, Duration::from_secs(20)).await?;
    ensure!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Prints what the log gained since `seen` bytes.
fn print_new_log(path: &Path, seen: &mut u64) {
    let Ok(bytes) = std::fs::read(path) else {
        return;
    };
    if (bytes.len() as u64) < *seen {
        *seen = 0; // cut
    }
    let new = &bytes[*seen as usize..];
    if let Some(end) = new.iter().rposition(|&b| b == b'\n') {
        for line in String::from_utf8_lossy(&new[..=end]).lines() {
            println!("  | {line}");
        }
        *seen += end as u64 + 1;
    }
}

/// `pumasi docker setup`.
pub async fn setup(opts: SetupOptions) -> Result<i32> {
    ensure!(
        elevated(),
        "`pumasi docker setup` changes accounts and scheduled tasks: run it from a terminal opened with \"Run as administrator\""
    );
    let arch = std::env::consts::ARCH;
    let rootfs = rootfs_pin(arch).with_context(|| format!("no WSL distro for {arch}"))?;
    let layout = Layout::system().context("no %ProgramData%")?;

    for dir in [
        layout.bin_dir(),
        layout.distro_dir(),
        layout.downloads(),
        layout.server_tls(),
        layout.client_tls(),
        layout.logs(),
    ] {
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    }
    // The account first: the ACLs name it.
    let password = new_password();
    if ensure_account(&password)? {
        println!("created the local account {ACCOUNT} (hidden from the sign-in screen)");
    } else {
        println!("reset the password of the local account {ACCOUNT}");
    }
    batch_logon(true)?;
    tool(
        "reg.exe",
        &[
            "add",
            USER_LIST,
            "/v",
            ACCOUNT,
            "/t",
            "REG_DWORD",
            "/d",
            "0",
            "/f",
        ],
    )
    .await?;
    restrict(&layout).await?;
    println!(
        "{} (readable by SYSTEM, administrators and {ACCOUNT})",
        layout.root().display()
    );

    let rootfs_file = download(&layout, &rootfs).await?;
    let cli_zip = download(&layout, &DOCKER_CLI).await?;

    // A running task holds bin\pumasi.exe; stop it before replacing that.
    let _ = schtasks(&["/End", "/TN", TASK]).await;
    extract_cli(&cli_zip, &layout).await?;
    let me = std::env::current_exe().context("find this binary")?;
    let copy = layout.pumasi_exe();
    let mut copied = false;
    for _ in 0..20 {
        if std::fs::copy(&me, &copy).is_ok() {
            copied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ensure!(
        copied,
        "could not copy {} to {}",
        me.display(),
        copy.display()
    );

    if opts.new_certificates || !tls::present(&layout.server_tls(), &layout.client_tls()) {
        let issued = tls::issue()?;
        tls::write(&issued, &layout.server_tls(), &layout.client_tls())?;
        println!("made a TLS CA, server and client certificate for the daemon");
    }
    let config = Config {
        port: opts.port,
        rootfs: rootfs.file.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    config.write(&layout)?;
    let _ = rootfs_file;

    std::fs::write(layout.task_xml(), utf16_file(&task_xml(&layout)))?;
    schtasks(&[
        "/Create",
        "/TN",
        TASK,
        "/XML",
        &layout.task_xml().display().to_string(),
        "/RU",
        ACCOUNT,
        "/RP",
        &password,
        "/F",
    ])
    .await
    .context("register the scheduled task")?;
    drop(password);
    println!("registered the scheduled task {TASK} (at boot, as {ACCOUNT})");
    // WSL last: without it, everything above still stands, and the task
    // starts Docker at the next boot, once WSL is installed.
    match wsl_version().await {
        Ok(v) => println!("WSL: {v}"),
        Err(e) => {
            println!(
                "WSL 2 is not installed ({e:#}); installing it (wsl --install --no-distribution)..."
            );
            let mut cmd = wsl();
            cmd.args(["--install", "--no-distribution"]);
            let out = output(cmd, Duration::from_secs(30 * 60)).await?;
            println!("{}", wsl_text(&out.stdout).trim());
            if !out.status.success() {
                bail!("installing WSL failed: {}", wsl_text(&out.stderr).trim());
            }
            println!(
                "WSL is installed. Restart Windows: the task then installs and starts Docker at boot (`pumasi docker status` shows how it goes)."
            );
            return Ok(3);
        }
    }
    schtasks(&["/Run", "/TN", TASK]).await?;
    println!("started it; the first start imports the distro and installs Docker (a few minutes)");

    let installed = Installed::at(layout.clone()).context("the setup is incomplete")?;
    if !opts.wait {
        println!("not waiting (--no-wait); `pumasi docker status` shows how it goes");
        return Ok(0);
    }
    let start = Instant::now();
    let mut seen = std::fs::metadata(layout.log_file())
        .map(|m| m.len())
        .unwrap_or(0);
    loop {
        print_new_log(&layout.log_file(), &mut seen);
        if let Ok(v) = docker_version(&installed).await {
            println!("Docker {v} answers in the {DISTRO} WSL distro");
            break;
        }
        if start.elapsed() > FIRST_START {
            bail!(
                "Docker did not answer within {} minutes; see {} and `pumasi docker status`",
                FIRST_START.as_secs() / 60,
                layout.log_file().display()
            );
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    println!(
        "done. An agent started with --allow-docker uses it (service: `pumasi service set -- --allow-docker`); the Docker CLI with its certificate is {}",
        layout.docker_exe().display()
    );
    Ok(0)
}

/// `pumasi docker status`. 0 when Docker answers.
pub async fn status() -> Result<i32> {
    let Some(layout) = Layout::system() else {
        bail!("no %ProgramData%");
    };
    let Some(installed) = Installed::at(layout.clone()) else {
        println!("not set up (`pumasi docker setup` in an administrator terminal)");
        return Ok(3);
    };
    let c = &installed.config;
    println!("directory: {}", layout.root().display());
    println!("set up by: pumasi {}", c.version);
    println!("distro:    {DISTRO} ({}), owned by {ACCOUNT}", c.rootfs);
    println!(
        "task:      {TASK}: {}",
        task_state()
            .await
            .unwrap_or_else(|| "not found (run `pumasi docker setup` again)".into())
    );
    let code = match docker_version(&installed).await {
        Ok(v) => {
            println!("docker:    {v} on tcp://127.0.0.1:{} (TLS)", c.port);
            let mut cmd = docker_command(&layout.docker_exe().display().to_string());
            cmd.args(["info", "--format", "{{json .Runtimes}}"]);
            if let Ok(out) = output(cmd, Duration::from_secs(20)).await {
                let gpu = String::from_utf8_lossy(&out.stdout).contains("nvidia");
                println!(
                    "gpu:       {}",
                    if gpu {
                        "NVIDIA Container Toolkit (the nvidia runtime)"
                    } else {
                        "none (no NVIDIA driver in WSL when it was set up)"
                    }
                );
            }
            0
        }
        Err(e) => {
            println!("docker:    not answering: {e:#}");
            3
        }
    };
    if let Ok(text) = std::fs::read_to_string(layout.log_file()) {
        println!("log:       {} (last lines)", layout.log_file().display());
        let lines: Vec<&str> = text.lines().collect();
        for l in &lines[lines.len().saturating_sub(12)..] {
            println!("  | {l}");
        }
    }
    Ok(code)
}

/// `pumasi docker remove`: the task, the distro (with the account's
/// profile, which registers it), the account and the files.
pub async fn remove() -> Result<i32> {
    ensure!(
        elevated(),
        "`pumasi docker remove` changes accounts and scheduled tasks: run it from a terminal opened with \"Run as administrator\""
    );
    let layout = Layout::system().context("no %ProgramData%")?;
    if schtasks(&["/End", "/TN", TASK]).await.is_ok() {
        println!("stopped the task");
    }
    if schtasks(&["/Delete", "/TN", TASK, "/F"]).await.is_ok() {
        println!("deleted the scheduled task {TASK}");
    }
    // The profile holds the distro's registration; Windows unloads it a
    // little after the task's logon ends.
    let script = format!(
        "Get-CimInstance Win32_UserProfile | Where-Object {{ $_.LocalPath -like '*\\{ACCOUNT}' }} | Remove-CimInstance -ErrorAction Stop"
    );
    let mut profile_gone = false;
    for _ in 0..24 {
        let mut cmd = Command::new(system32(r"WindowsPowerShell\v1.0\powershell.exe"));
        cmd.args(["-NoProfile", "-NonInteractive", "-Command", &script]);
        if output(cmd, Duration::from_secs(60))
            .await
            .is_ok_and(|o| o.status.success())
        {
            profile_gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    if profile_gone {
        println!("removed the profile of {ACCOUNT} (and with it the distro's registration)");
    } else {
        println!(
            "could not remove the profile of {ACCOUNT} yet (it is in use); restart Windows and run `pumasi docker remove` again to finish"
        );
    }
    if let Err(e) = batch_logon(false) {
        println!("{e:#}");
    }
    if delete_account()? {
        println!("deleted the local account {ACCOUNT}");
    }
    let _ = tool("reg.exe", &["delete", USER_LIST, "/v", ACCOUNT, "/f"]).await;
    // The VM lets go of the distro's disk once it has shut down.
    let mut removed = !layout.root().exists();
    for _ in 0..30 {
        if removed {
            break;
        }
        removed = std::fs::remove_dir_all(layout.root()).is_ok() || !layout.root().exists();
        if !removed {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    if removed {
        println!("removed {}", layout.root().display());
        Ok(0)
    } else {
        println!(
            "could not remove {} yet (the WSL VM still holds its disk); restart Windows and delete it",
            layout.root().display()
        );
        Ok(1)
    }
}

/// Starts the task, at most every two minutes: the agent's way to bring
/// Docker back when it stops answering. Task Scheduler ignores it while
/// the task runs.
pub fn kick() {
    static LAST: Mutex<Option<Instant>> = Mutex::new(None);
    let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
    if last.is_some_and(|t| t.elapsed() < Duration::from_secs(120)) {
        return;
    }
    *last = Some(Instant::now());
    let _ = std::process::Command::new(system32("schtasks.exe"))
        .args(["/Run", "/TN", TASK])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

/// The task's log, cut at [`LOG_LIMIT`].
struct Log {
    path: PathBuf,
    file: std::fs::File,
    len: u64,
}

impl Log {
    fn open(path: PathBuf) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        let len = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self { path, file, len })
    }

    fn line(&mut self, text: &str) {
        let stamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let line = format!("{stamp} {}\n", text.trim_end());
        let _ = self.file.write_all(line.as_bytes());
        self.len += line.len() as u64;
        if self.len > LOG_LIMIT {
            let old = self.path.with_extension("log.1");
            let _ = std::fs::remove_file(&old);
            let _ = std::fs::rename(&self.path, &old);
            if let Ok(next) = Self::open(self.path.clone()) {
                *self = next;
            }
        }
    }
}

type SharedLog = Arc<Mutex<Log>>;

fn say(log: &SharedLog, text: &str) {
    log.lock().unwrap_or_else(|e| e.into_inner()).line(text);
}

/// Runs `wsl.exe args` (stdin from `input`), its output into the log.
async fn wsl_logged(log: &SharedLog, args: &[&str], input: Option<&str>) -> Result<bool> {
    let mut cmd = wsl();
    cmd.args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().context("start wsl.exe")?;
    // When the task is ended, wsl.exe (and with it the distro) goes too.
    let _job = crate::platform::ProcessJob::new()
        .and_then(|j| j.assign(&child).map(|()| j))
        .ok();
    if let (Some(text), Some(mut stdin)) = (input, child.stdin.take()) {
        stdin.write_all(text.as_bytes()).await?;
        drop(stdin);
    }
    let pump = |r: Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>| {
        let log = log.clone();
        tokio::spawn(async move {
            let Some(r) = r else { return };
            let mut lines = BufReader::new(r).split(b'\n');
            while let Ok(Some(line)) = lines.next_segment().await {
                let text = wsl_text(&line);
                if !text.trim().is_empty() {
                    say(&log, &text);
                }
            }
        })
    };
    let a = pump(
        child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>),
    );
    let b = pump(
        child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>),
    );
    let status = child.wait().await?;
    let _ = tokio::join!(a, b);
    Ok(status.success())
}

/// Runs `wsl.exe args` and returns its output (for short checks).
async fn wsl_out(args: &[&str]) -> Result<std::process::Output> {
    let mut cmd = wsl();
    cmd.args(args);
    output(cmd, Duration::from_secs(300)).await
}

/// `pumasi __wsl-docker-run --dir DIR`, the task's program (as
/// [`ACCOUNT`]): imports and sets up the distro when needed, then keeps
/// `dockerd` running, starting it again after a pause when it ends.
pub async fn run_task(dir: &Path) -> Result<i32> {
    let layout = Layout::new(dir);
    let log: SharedLog = Arc::new(Mutex::new(Log::open(layout.log_file())?));
    say(
        &log,
        &format!(
            "pumasi {} starts the {DISTRO} distro as {}",
            env!("CARGO_PKG_VERSION"),
            std::env::var("USERNAME").unwrap_or_default()
        ),
    );
    let config = Config::read(&layout)?;
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        let path = PathBuf::from(profile).join(".wslconfig");
        if std::fs::read_to_string(&path).ok().as_deref() != Some(WSL_CONFIG)
            && let Err(e) = std::fs::write(&path, WSL_CONFIG)
        {
            say(&log, &format!("could not write {}: {e}", path.display()));
        }
    }
    let mut pause = Duration::from_secs(5);
    loop {
        let started = Instant::now();
        match run_once(&layout, &config, &log).await {
            Ok(true) => say(&log, "dockerd stopped"),
            Ok(false) => say(&log, "dockerd ended with an error"),
            Err(e) => say(&log, &format!("error: {e:#}")),
        }
        if started.elapsed() > Duration::from_secs(600) {
            pause = Duration::from_secs(5);
        }
        say(&log, &format!("starting again in {} s", pause.as_secs()));
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(Duration::from_secs(120));
    }
}

/// One start: import, set up, run dockerd until it ends.
async fn run_once(layout: &Layout, config: &Config, log: &SharedLog) -> Result<bool> {
    let list = wsl_out(&["--list", "--quiet"]).await?;
    let vhdx = layout.distro_dir().join("ext4.vhdx");
    if !lists_distro(&list.stdout) && vhdx.is_file() {
        // The disk outlived its registration (a profile that was reset):
        // register it again, images and all.
        say(
            log,
            &format!(
                "registering {} again as the {DISTRO} distro",
                vhdx.display()
            ),
        );
        let ok = wsl_logged(
            log,
            &["--import-in-place", DISTRO, &vhdx.display().to_string()],
            None,
        )
        .await?;
        ensure!(ok, "wsl --import-in-place failed (see above)");
    } else if !lists_distro(&list.stdout) {
        let rootfs = layout.downloads().join(&config.rootfs);
        say(
            log,
            &format!("importing {} as the {DISTRO} distro", rootfs.display()),
        );
        let ok = wsl_logged(
            log,
            &[
                "--import",
                DISTRO,
                &layout.distro_dir().display().to_string(),
                &rootfs.display().to_string(),
                "--version",
                "2",
            ],
            None,
        )
        .await?;
        ensure!(
            ok,
            "wsl --import failed (is WSL 2 working on this machine? see above)"
        );
    }
    let gpu = wsl_out(&["-d", DISTRO, "-u", "root", "--exec", "test", "-e", WSL_CUDA])
        .await?
        .status
        .success();
    let server = tls::read_server(&layout.server_tls())?;
    let marker = provision_marker(&server, gpu);
    let current = wsl_out(&["-d", DISTRO, "-u", "root", "--exec", "cat", MARKER_FILE]).await?;
    if wsl_text(&current.stdout).trim() != marker {
        say(log, &format!("setting up the distro ({marker})"));
        let script = provision_script(&server, config.port, gpu, &marker);
        let ok = wsl_logged(
            log,
            &["-d", DISTRO, "-u", "root", "--exec", "sh", "-s"],
            Some(&script),
        )
        .await?;
        ensure!(ok, "setting up the distro failed (see above)");
        // wsl.conf applies from the distro's next start.
        let _ = wsl_out(&["--terminate", DISTRO]).await;
    }
    say(
        log,
        &format!("starting dockerd on 127.0.0.1:{} (TLS)", config.port),
    );
    wsl_logged(log, &start_command(), None).await
}
