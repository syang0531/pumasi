//! The init process. Runs once per VM boot, writes a result to the output
//! disk, and reboots or powers off (`pumasi.exit`), which ends the VMM
//! process on the host.
//!
//! The init is single-threaded on purpose: it forks the task (or the layer
//! unpacker) and the child may then allocate freely before `execve`.

use std::{
    collections::HashSet,
    ffi::{CString, OsStr},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{FileExt, OpenOptionsExt},
        fs::{PermissionsExt, chroot},
        io::{AsRawFd, FromRawFd},
    },
    path::{Component, Path, PathBuf},
};

use pumasi_guest::{
    BASE_LABEL, DISK_ROLE_ENTRY, GPU_LABEL, GuestOutcome, GuestResult, INPUT_PREFIX,
    MAX_OUTPUT_FILES, MAX_OUTPUT_FILES_BYTES, MAX_STREAM_BYTES, MODE_PARAM, OUTPUT_MARK,
    OUTPUT_PREFIX, RESULT_ENTRY, ROLE_INPUT, ROLE_LAYERS, RunSpec, SCRATCH_LABEL, SPEC_ENTRY,
    STDERR_ENTRY, STDIN_ENTRY, STDOUT_ENTRY,
    seal::{
        self, AttestReply, AttestRequest, EVIDENCE_AT, Evidence, Kind, REPLY_AT, SealReader,
        SealWriter, TaskKeys, TeeKind,
    },
};

type R<T> = Result<T, String>;

trait Ctx<T> {
    fn ctx(self, what: &str) -> R<T>;
}

impl<T, E: std::fmt::Display> Ctx<T> for Result<T, E> {
    fn ctx(self, what: &str) -> R<T> {
        self.map_err(|e| format!("{what}: {e}"))
    }
}

fn log(msg: &str) {
    let line = format!("pumasi-guest: {msg}\n");
    // Never panic here: this is also the panic path. When the console is
    // not writable, the kernel log still reaches the serial console.
    if io::stderr().write_all(line.as_bytes()).is_err()
        && let Ok(mut k) = OpenOptions::new().write(true).open("/dev/kmsg")
    {
        let _ = k.write_all(format!("<2>{line}").as_bytes());
    }
}

pub fn main() -> ! {
    std::panic::set_hook(Box::new(|info| log(&format!("panic: {info}"))));
    if let Err(p) = std::panic::catch_unwind(boot) {
        let msg = p
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        log(&format!("panic: {msg}"));
    }
    power_off()
}

fn power_off() -> ! {
    let off =
        cmdline_param(pumasi_guest::EXIT_PARAM).as_deref() == Some(pumasi_guest::EXIT_POWER_OFF);
    unsafe { libc::sync() };
    // `pumasi_guest::DONE_LINE`: the host watches the console for it, in
    // case the VM keeps running.
    log("done");
    unsafe {
        // With `reboot=k` (Firecracker) or `-no-reboot` (QEMU) a reboot
        // ends the VMM process; Virtualization.framework reboots the VM
        // instead, so the vz backend asks for a power off.
        libc::reboot(if off {
            libc::RB_POWER_OFF
        } else {
            libc::RB_AUTOBOOT
        });
    }
    loop {
        std::thread::park();
    }
}

/// What to put on the output disk.
struct Payload {
    result: GuestResult,
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    /// Output file limits (bytes, count).
    limits: (u64, usize),
}

const DEFAULT_LIMITS: (u64, usize) = (MAX_OUTPUT_FILES_BYTES, MAX_OUTPUT_FILES);

impl Payload {
    fn failed(msg: String) -> Self {
        log(&msg);
        Self {
            result: GuestResult::failed(msg),
            stdout: None,
            stderr: None,
            output_dir: None,
            limits: DEFAULT_LIMITS,
        }
    }
}

fn boot() {
    for (src, dir, fs) in [
        ("devtmpfs", "/dev", "devtmpfs"),
        ("proc", "/proc", "proc"),
        ("sysfs", "/sys", "sysfs"),
    ] {
        let _ = fs::create_dir_all(dir);
        if let Err(e) = mount(src, dir, fs, 0, "") {
            log(&e);
        }
    }
    // The task's out-of-memory kills must never take the init with them.
    let _ = fs::write("/proc/self/oom_score_adj", "-1000");
    let mode = cmdline_param(MODE_PARAM).unwrap_or_default();
    let disks = match find_disks() {
        Ok(d) => d,
        Err(e) => {
            log(&format!("cannot list disks: {e}"));
            return;
        }
    };
    let Some(output) = disks.output.clone() else {
        log("no output disk");
        return;
    };
    let mut seal_out = None;
    let payload = match mode.as_str() {
        "build" => build(&disks),
        "run" => run(&disks),
        "probe" => probe(&disks),
        "sealed" => {
            let (p, s) = sealed(&disks);
            seal_out = s;
            p
        }
        other => Payload::failed(format!("unknown mode {other:?}")),
    };
    if let Err(e) = write_output(&output, payload, seal_out.as_ref()) {
        log(&format!("cannot write the result: {e}"));
    }
}

fn cmdline_param(name: &str) -> Option<String> {
    let line = fs::read_to_string("/proc/cmdline").ok()?;
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('=').map(String::from))
}

// ---------------------------------------------------------------------------
// Disks
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Disks {
    base: Option<String>,
    scratch: Option<String>,
    layers: Option<String>,
    input: Option<String>,
    output: Option<String>,
    gpu: Option<String>,
    /// Confidential tasks: the attest disk and the sealed layers and input.
    attest: Option<String>,
    sealed_layers: Option<String>,
    sealed_input: Option<String>,
}

fn find_disks() -> io::Result<Disks> {
    let mut d = Disks::default();
    let mut names: Vec<String> = fs::read_dir("/sys/block")?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("vd"))
        .collect();
    names.sort();
    for name in names {
        let dev = format!("/dev/{name}");
        let mut head = vec![0u8; 4096];
        let n = match File::open(&dev).and_then(|mut f| read_full(&mut f, &mut head)) {
            Ok(n) => n,
            Err(e) => {
                log(&format!("cannot read {dev}: {e}"));
                continue;
            }
        };
        let head = &head[..n];
        if head.starts_with(OUTPUT_MARK) {
            d.output = Some(dev);
        } else if let Some(kind) = seal::sealed_kind(head) {
            match kind {
                Kind::Layers => d.sealed_layers = Some(dev),
                Kind::Input => d.sealed_input = Some(dev),
                Kind::Output => log(&format!("{dev}: a sealed output where none belongs")),
            }
        } else if AttestRequest::decode(head).is_some() {
            d.attest = Some(dev);
        } else if let Some(label) = ext4_label(head) {
            match label.as_str() {
                BASE_LABEL => d.base = Some(dev),
                SCRATCH_LABEL => d.scratch = Some(dev),
                GPU_LABEL => d.gpu = Some(dev),
                other => log(&format!("{dev}: unknown filesystem label {other:?}")),
            }
        } else if let Some(role) = tar_role(head) {
            match role.as_str() {
                ROLE_LAYERS => d.layers = Some(dev),
                ROLE_INPUT => d.input = Some(dev),
                other => log(&format!("{dev}: unknown disk role {other:?}")),
            }
        }
    }
    Ok(d)
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

fn ext4_label(head: &[u8]) -> Option<String> {
    if head.len() < 1160 || head[1080..1082] != [0x53, 0xEF] {
        return None;
    }
    let raw = &head[1144..1160];
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    Some(String::from_utf8_lossy(&raw[..end]).into_owned())
}

fn tar_role(head: &[u8]) -> Option<String> {
    let mut ar = tar::Archive::new(head);
    let mut e = ar.entries().ok()?.next()?.ok()?;
    if e.path().ok()?.as_os_str() != DISK_ROLE_ENTRY {
        return None;
    }
    let mut role = String::new();
    e.read_to_string(&mut role).ok()?;
    Some(role.trim().to_string())
}

// ---------------------------------------------------------------------------
// Syscall helpers
// ---------------------------------------------------------------------------

fn cstr(s: impl AsRef<OsStr>) -> CString {
    CString::new(s.as_ref().as_bytes()).unwrap_or_default()
}

fn mount(src: &str, dst: &str, fstype: &str, flags: libc::c_ulong, data: &str) -> R<()> {
    let (s, d, t, o) = (cstr(src), cstr(dst), cstr(fstype), cstr(data));
    let rc = unsafe {
        libc::mount(
            s.as_ptr(),
            d.as_ptr(),
            t.as_ptr(),
            flags,
            if data.is_empty() {
                std::ptr::null()
            } else {
                o.as_ptr().cast()
            },
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!(
            "mount {src} on {dst} ({fstype}): {}",
            io::Error::last_os_error()
        ))
    }
}

fn umount(dst: &str) {
    let d = cstr(dst);
    unsafe {
        libc::umount2(d.as_ptr(), 0);
    }
}

fn waitpid(pid: libc::pid_t) -> io::Result<(libc::pid_t, libc::c_int)> {
    let mut status = 0;
    loop {
        let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
        if rc >= 0 {
            return Ok((rc, status));
        }
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EINTR) {
            return Err(e);
        }
    }
}

fn fork() -> R<libc::pid_t> {
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        Err(format!("fork: {}", io::Error::last_os_error()))
    } else {
        Ok(pid)
    }
}

fn exit_now(code: i32) -> ! {
    unsafe { libc::_exit(code) }
}

fn oom_kills() -> u64 {
    fs::read_to_string("/proc/vmstat")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("oom_kill ")?.trim().parse().ok())
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Probe mode: copy the input files to the output, to check disk I/O
// ---------------------------------------------------------------------------

fn probe(d: &Disks) -> Payload {
    let dir = Path::new("/probe");
    let copied = d
        .input
        .as_deref()
        .ok_or_else(|| "no input disk".to_string())
        .and_then(|input| {
            fs::create_dir_all(TASK_DIR).ctx(TASK_DIR)?;
            let input = File::open(input).ctx("open input disk")?;
            read_input(input, dir).map(|_| ())
        });
    match copied {
        Ok(()) => Payload {
            result: GuestResult {
                outcome: GuestOutcome::Exited(0),
                stdout_dropped: 0,
                stderr_dropped: 0,
                files_dropped: 0,
            },
            stdout: None,
            stderr: None,
            output_dir: Some(dir.to_path_buf()),
            limits: DEFAULT_LIMITS,
        },
        Err(e) => Payload::failed(e),
    }
}

// ---------------------------------------------------------------------------
// Build mode: unpack the image layers onto the empty base disk
// ---------------------------------------------------------------------------

fn build(d: &Disks) -> Payload {
    match build_inner(d) {
        Ok(()) => Payload {
            result: GuestResult {
                outcome: GuestOutcome::Exited(0),
                stdout_dropped: 0,
                stderr_dropped: 0,
                files_dropped: 0,
            },
            stdout: None,
            stderr: None,
            output_dir: None,
            limits: DEFAULT_LIMITS,
        },
        Err(e) => Payload::failed(e),
    }
}

fn build_inner(d: &Disks) -> R<()> {
    let base = d.base.as_deref().ok_or("no base disk")?;
    let layers = d.layers.as_deref().ok_or("no layers disk")?;
    fs::create_dir_all("/newroot").ctx("mkdir /newroot")?;
    mount(base, "/newroot", "ext4", 0, "")?;
    let layers = File::open(layers).ctx("open layers disk")?;
    let res = unpack_in_chroot(layers);
    unsafe { libc::sync() };
    umount("/newroot");
    res
}

/// Unpacks the layers tar read from `layers` into `/newroot`, in a child
/// chrooted there.
fn unpack_in_chroot(layers: impl Read) -> R<()> {
    let mut fds = [0; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(format!("pipe: {}", io::Error::last_os_error()));
    }
    // Unpack in a child that chroots into the new root, so absolute
    // symlinks inside the image resolve inside it. The parent stays outside
    // to unmount cleanly afterwards.
    let pid = fork()?;
    if pid == 0 {
        unsafe { libc::close(fds[0]) };
        let mut report = unsafe { File::from_raw_fd(fds[1]) };
        let res = chroot("/newroot")
            .ctx("chroot")
            .and_then(|_| std::env::set_current_dir("/").ctx("chdir"))
            .and_then(|_| unpack_layers(layers));
        match res {
            Ok(()) => exit_now(0),
            Err(e) => {
                let _ = report.write_all(e.as_bytes());
                exit_now(1)
            }
        }
    }
    drop(layers);
    unsafe { libc::close(fds[1]) };
    let mut msg = String::new();
    let _ = unsafe { File::from_raw_fd(fds[0]) }.read_to_string(&mut msg);
    let (_, status) = waitpid(pid).ctx("wait for unpacker")?;
    if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
        Ok(())
    } else if msg.is_empty() {
        Err(format!("layer unpacker died (status {status})"))
    } else {
        Err(msg)
    }
}

fn unpack_layers(disk: impl Read) -> R<()> {
    let mut outer = tar::Archive::new(BufReader::with_capacity(1 << 20, disk));
    let mut index = 0;
    for entry in outer.entries().ctx("read layers disk")? {
        let entry = entry.ctx("read layers disk")?;
        if entry.path().ctx("layer name")?.as_os_str() == DISK_ROLE_ENTRY {
            continue;
        }
        let mut br = BufReader::with_capacity(1 << 20, entry);
        let magic = br.fill_buf().ctx("read layer")?.to_vec();
        let reader: Box<dyn Read> = if magic.starts_with(&[0x1f, 0x8b]) {
            Box::new(flate2::read::MultiGzDecoder::new(br))
        } else if magic.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            Box::new(
                ruzstd::decoding::StreamingDecoder::new(br)
                    .map_err(|e| format!("layer {index}: zstd: {e}"))?,
            )
        } else {
            Box::new(br)
        };
        apply_layer(reader).map_err(|e| format!("layer {index}: {e}"))?;
        index += 1;
    }
    Ok(())
}

/// A layer path relative to the root, or `None` if it tries to leave it.
fn layer_path(p: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(s) => out.push(s),
            Component::CurDir | Component::RootDir => {}
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn remove_any(p: &Path) -> io::Result<()> {
    match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => fs::remove_dir_all(p),
        Ok(_) => fs::remove_file(p),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Applies one layer tarball to `/` (the chroot), honoring OCI whiteouts.
fn apply_layer(r: impl Read) -> R<()> {
    let mut ar = tar::Archive::new(r);
    ar.set_preserve_permissions(true);
    ar.set_preserve_ownerships(true);
    ar.set_preserve_mtime(true);
    ar.set_overwrite(true);
    // Paths this layer wrote, which an opaque whiteout later in the same
    // layer must keep.
    let mut written: HashSet<PathBuf> = HashSet::new();
    for entry in ar.entries().ctx("read layer")? {
        let mut entry = entry.ctx("read layer")?;
        let raw = entry.path().ctx("entry path")?.into_owned();
        let Some(rel) = layer_path(&raw) else {
            continue;
        };
        let name = rel.file_name().map(|n| n.as_bytes()).unwrap_or_default();
        let parent = Path::new("/").join(rel.parent().unwrap_or(Path::new("")));
        if name == b".wh..wh..opq" {
            clear_lower(&parent, &written).ctx("opaque whiteout")?;
            continue;
        }
        if let Some(hidden) = name.strip_prefix(b".wh.") {
            remove_any(&parent.join(OsStr::from_bytes(hidden))).ctx("whiteout")?;
            continue;
        }
        let target = Path::new("/").join(&rel);
        let kind = entry.header().entry_type();
        if let Ok(meta) = fs::symlink_metadata(&target)
            && !(meta.is_dir() && kind.is_dir())
        {
            remove_any(&target).ctx("replace entry")?;
        }
        let special =
            !(kind.is_file() || kind.is_dir() || kind.is_symlink() || kind.is_hard_link());
        match entry.unpack_in("/") {
            Ok(_) => {}
            // Device nodes and the like are not needed (devtmpfs provides
            // /dev); skip what cannot be created.
            Err(e) if special => log(&format!("skipping {}: {e}", rel.display())),
            Err(e) => return Err(format!("{}: {e}", rel.display())),
        }
        let mut p = rel.as_path();
        loop {
            written.insert(p.to_path_buf());
            match p.parent() {
                Some(q) if !q.as_os_str().is_empty() => p = q,
                _ => break,
            }
        }
    }
    Ok(())
}

/// Removes what lower layers put under `dir`, keeping what this layer wrote.
fn clear_lower(dir: &Path, written: &HashSet<PathBuf>) -> io::Result<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };
    for e in entries {
        let path = e?.path();
        let rel = path.strip_prefix("/").unwrap_or(&path).to_path_buf();
        if written.contains(&rel) {
            if fs::symlink_metadata(&path)?.is_dir() {
                clear_lower(&path, written)?;
            }
        } else {
            remove_any(&path)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Run mode
// ---------------------------------------------------------------------------

const ROOT: &str = "/newroot";
const TASK_DIR: &str = "/scratch/task";
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

fn run(d: &Disks) -> Payload {
    match run_inner(d) {
        Ok(p) => p,
        Err(e) => Payload::failed(e),
    }
}

fn run_inner(d: &Disks) -> R<Payload> {
    let base = d.base.as_deref().ok_or("no base disk")?;
    let scratch = d.scratch.as_deref().ok_or("no scratch disk")?;
    let input = d.input.as_deref().ok_or("no input disk")?;
    for dir in ["/lower", "/scratch", ROOT] {
        fs::create_dir_all(dir).ctx(dir)?;
    }
    mount(base, "/lower", "ext4", libc::MS_RDONLY, "")?;
    mount(scratch, "/scratch", "ext4", 0, "")?;
    for dir in ["/scratch/upper", "/scratch/work", TASK_DIR] {
        fs::create_dir_all(dir).ctx(dir)?;
    }
    mount(
        "overlay",
        ROOT,
        "overlay",
        0,
        "lowerdir=/lower,upperdir=/scratch/upper,workdir=/scratch/work",
    )?;
    let input = File::open(input).ctx("open input disk")?;
    run_in_root(d, input)
}

/// Reads the input into the root at [`ROOT`], runs the command and
/// collects the result.
fn run_in_root(d: &Disks, input: impl Read) -> R<Payload> {
    let root = Path::new(ROOT);
    let input_dir = root.join("input");
    ensure_dir(&input_dir, 0o755)?;
    let spec = read_input(input, &input_dir)?;
    if spec.argv.is_empty() {
        return Err("nothing to run: the image has no command and the job gave none".into());
    }
    let (uid, gid, home) = resolve_user(root, &spec.user)?;
    prepare_root(root, &spec, uid, gid)?;
    if let Some(gpu) = d.gpu.as_deref() {
        gpu_setup(gpu, root)?;
    }

    let mut env = spec.env.clone();
    if !env.iter().any(|e| e.starts_with("PATH=")) {
        env.push(format!("PATH={DEFAULT_PATH}"));
    }
    if d.gpu.is_some() {
        add_to_list(&mut env, "PATH", NVIDIA_BIN, false);
        add_to_list(&mut env, "LD_LIBRARY_PATH", NVIDIA_LIB, true);
    }
    if !env.iter().any(|e| e.starts_with("HOME=")) {
        env.push(format!("HOME={home}"));
    }
    let path_var = env
        .iter()
        .find_map(|e| e.strip_prefix("PATH="))
        .unwrap_or(DEFAULT_PATH)
        .to_string();

    let stdout_path = PathBuf::from(TASK_DIR).join("stdout");
    let stderr_path = PathBuf::from(TASK_DIR).join("stderr");
    let stdin = File::open(Path::new(TASK_DIR).join("stdin"))
        .or_else(|_| File::open("/dev/null"))
        .ctx("open stdin")?;
    let stdout = File::create(&stdout_path).ctx("create stdout")?;
    let stderr = File::create(&stderr_path).ctx("create stderr")?;
    let argv: Vec<CString> = spec.argv.iter().map(cstr).collect();
    let envp: Vec<CString> = env.iter().map(cstr).collect();

    let oom_before = oom_kills();
    let pid = fork()?;
    if pid == 0 {
        child(
            &stdin,
            &stdout,
            &stderr,
            &spec.workdir,
            uid,
            gid,
            &path_var,
            argv,
            envp,
        );
    }
    drop((stdin, stdout, stderr));
    let status = loop {
        match waitpid(-1) {
            Ok((p, st)) if p == pid => break st,
            Ok(_) => continue,
            Err(e) => return Err(format!("wait for the task: {e}")),
        }
    };
    // The command is done; end whatever it left running.
    unsafe { libc::kill(-1, libc::SIGKILL) };
    while waitpid(-1).is_ok() {}
    let oom = oom_kills() > oom_before;
    let outcome = if libc::WIFEXITED(status) {
        match libc::WEXITSTATUS(status) {
            0 => GuestOutcome::Exited(0),
            _ if oom => GuestOutcome::OutOfMemory,
            code => GuestOutcome::Exited(code),
        }
    } else if libc::WIFSIGNALED(status) {
        if oom {
            GuestOutcome::OutOfMemory
        } else {
            GuestOutcome::Signaled(libc::WTERMSIG(status))
        }
    } else {
        GuestOutcome::Failed(format!("unexpected wait status {status}"))
    };
    unsafe { libc::sync() };
    Ok(Payload {
        result: GuestResult {
            outcome,
            stdout_dropped: 0,
            stderr_dropped: 0,
            files_dropped: 0,
        },
        stdout: Some(stdout_path),
        stderr: Some(stderr_path),
        output_dir: Some(root.join("output")),
        limits: (spec.files_limit(), spec.count_limit()),
    })
}

// ---------------------------------------------------------------------------
// Sealed mode (confidential tasks, phase 16)
// ---------------------------------------------------------------------------

/// How long the guest waits for the cluster to release its keys.
const KEY_WAIT_SECS: u64 = 600;

/// Attests, unpacks the sealed layers into memory, reads the sealed input
/// and runs the command. The result is sealed whenever the keys came: from
/// then on nothing leaves the VM in the clear. Without them, only why goes
/// out.
fn sealed(d: &Disks) -> (Payload, Option<OutputSeal>) {
    let (nonce, keys) = match attest(d) {
        Ok(k) => k,
        Err(e) => return (Payload::failed(e), None),
    };
    let seal = OutputSeal {
        key: keys.task,
        nonce,
    };
    let p = match sealed_inner(d, &keys) {
        Ok(p) => p,
        Err(e) => Payload::failed(e),
    };
    (p, Some(seal))
}

fn sealed_inner(d: &Disks, keys: &TaskKeys) -> R<Payload> {
    let layers = d.sealed_layers.as_deref().ok_or("no sealed layers disk")?;
    let input = d.sealed_input.as_deref().ok_or("no sealed input disk")?;
    // The image and what the task writes live in memory, which the TEE
    // encrypts: the host's disks see only sealed bytes.
    for dir in [ROOT, "/scratch"] {
        fs::create_dir_all(dir).ctx(dir)?;
        mount("tmpfs", dir, "tmpfs", 0, "mode=0755,size=90%")?;
    }
    fs::create_dir_all(TASK_DIR).ctx(TASK_DIR)?;
    let layers = File::open(layers).ctx("open sealed layers disk")?;
    let layers = SealReader::new(layers, &keys.image, Kind::Layers, b"").ctx("sealed layers")?;
    unpack_in_chroot(layers)?;
    let input = File::open(input).ctx("open sealed input disk")?;
    let input = SealReader::new(input, &keys.task, Kind::Input, b"").ctx("sealed input")?;
    run_in_root(d, input)
}

/// Asks the TEE for a report binding a new key pair to the run's nonce,
/// hands it to the host, and waits for the keys sealed to that key pair.
fn attest(d: &Disks) -> R<([u8; 32], TaskKeys)> {
    let dev = d.attest.as_deref().ok_or("no attest disk")?;
    let head = read_direct(dev, 0, 4096).ctx("read attest disk")?;
    let req = AttestRequest::decode(&head).ok_or("bad attest request")?;
    let (secret, public) = seal::key_pair();
    let rd = seal::report_data(&req.nonce, &public);
    let evidence = match req.tee {
        // No hardware: the "measurement" is the init's own hash, so the
        // trust flow (an admin approves what was seen) works the same.
        TeeKind::Test => Evidence {
            tee: TeeKind::Test,
            report: {
                use sha2::Digest;
                let init = fs::read("/init").ctx("read /init")?;
                sha2::Sha384::digest(&init).to_vec()
            },
            aux: Vec::new(),
            key: public,
            report_data: rd.to_vec(),
        },
        TeeKind::SevSnp | TeeKind::Tdx => {
            let (tee, report, aux) = tsm_report(&rd)?;
            Evidence {
                tee,
                report,
                aux,
                key: public,
                report_data: Vec::new(),
            }
        }
    };
    log(&format!("attesting ({:?})", evidence.tee));
    write_direct(dev, EVIDENCE_AT, &evidence.frame()).ctx("write evidence")?;
    let start = std::time::Instant::now();
    loop {
        let b = read_direct(dev, REPLY_AT, 4096).ctx("read attest disk")?;
        match AttestReply::unframe(&b) {
            Some(AttestReply::Keys(w)) => {
                let keys = seal::unwrap_keys(&secret, &req.nonce, &w).ctx("open the keys")?;
                log("keys released");
                return Ok((req.nonce, keys));
            }
            Some(AttestReply::Refused(why)) => {
                return Err(format!("the cluster did not release the keys: {why}"));
            }
            None if start.elapsed().as_secs() > KEY_WAIT_SECS => {
                return Err("no answer from the cluster about the keys".into());
            }
            None => std::thread::sleep(std::time::Duration::from_millis(200)),
        }
    }
}

/// A report from the kernel's TSM interface (configfs, Linux 6.7+): the
/// SEV-SNP attestation report or the TDX quote, with `report_data` in it.
fn tsm_report(report_data: &[u8; 64]) -> R<(TeeKind, Vec<u8>, Vec<u8>)> {
    let cfg = "/sys/kernel/config";
    if !Path::new(cfg).join("tsm").exists() {
        let _ = mount("configfs", cfg, "configfs", 0, "");
    }
    let dir = Path::new(cfg).join("tsm/report/pumasi");
    if !dir.exists() {
        fs::create_dir(&dir).ctx(
            "create a TSM report (needs a kernel with CONFIG_TSM_REPORTS and a SEV-SNP or TDX guest)",
        )?;
    }
    fs::write(dir.join("inblob"), report_data).ctx("write the report data")?;
    let provider = fs::read_to_string(dir.join("provider")).ctx("read the TSM provider")?;
    let tee = match provider.trim() {
        "sev_guest" => TeeKind::SevSnp,
        "tdx_guest" => TeeKind::Tdx,
        other => return Err(format!("unknown TSM provider {other:?}")),
    };
    let report = fs::read(dir.join("outblob")).ctx("get the report")?;
    let aux = fs::read(dir.join("auxblob")).unwrap_or_default();
    Ok((tee, report, aux))
}

/// A 4 KiB-aligned buffer of `len` bytes (`O_DIRECT` needs one).
struct Aligned {
    buf: Vec<u8>,
    at: usize,
    len: usize,
}

impl Aligned {
    fn new(len: usize) -> Self {
        let buf = vec![0u8; len + 4096];
        let at = buf.as_ptr().align_offset(4096);
        Self { buf, at, len }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.at..self.at + self.len]
    }
}

/// Reads from the attest disk past the page cache, so what the host wrote
/// since is seen.
fn read_direct(dev: &str, at: u64, len: usize) -> io::Result<Vec<u8>> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(dev)?;
    let mut b = Aligned::new(len);
    let mut n = 0;
    while n < len {
        match f.read_at(&mut b.as_mut()[n..], at + n as u64)? {
            0 => break,
            k => n += k,
        }
    }
    Ok(b.as_mut()[..n].to_vec())
}

fn write_direct(dev: &str, at: u64, data: &[u8]) -> io::Result<()> {
    let f = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT | libc::O_SYNC)
        .open(dev)?;
    let mut b = Aligned::new(data.len().div_ceil(4096) * 4096);
    b.as_mut()[..data.len()].copy_from_slice(data);
    f.write_all_at(b.as_mut(), at)
}

/// Where the GPU driver's libraries and tools appear in the task's root
/// (the paths NVIDIA's CUDA images already search).
const NVIDIA_LIB: &str = "/usr/local/nvidia/lib64";
const NVIDIA_BIN: &str = "/usr/local/nvidia/bin";

/// Adds `dir` to a `:`-separated variable unless it is there.
fn add_to_list(env: &mut Vec<String>, var: &str, dir: &str, front: bool) {
    let prefix = format!("{var}=");
    match env.iter_mut().find(|e| e.starts_with(&prefix)) {
        Some(e) => {
            let cur = e[prefix.len()..].to_string();
            if !cur.split(':').any(|d| d == dir) {
                *e = if cur.is_empty() {
                    format!("{prefix}{dir}")
                } else if front {
                    format!("{prefix}{dir}:{cur}")
                } else {
                    format!("{prefix}{cur}:{dir}")
                };
            }
        }
        None => env.push(format!("{prefix}{dir}")),
    }
}

/// Loads the GPU driver from its disk, makes its device nodes and mounts
/// its libraries and tools into the task's root.
fn gpu_setup(dev: &str, root: &Path) -> R<()> {
    fs::create_dir_all("/gpu").ctx("/gpu")?;
    mount(dev, "/gpu", "ext4", libc::MS_RDONLY, "")?;
    let order =
        fs::read_to_string("/gpu/modules.order").ctx("read the GPU driver's module list")?;
    for m in order
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        if m.split('/').any(|c| c == "..") {
            return Err(format!("bad module path {m:?}"));
        }
        let f = File::open(Path::new("/gpu/modules").join(m)).ctx(m)?;
        let r = unsafe {
            libc::syscall(
                libc::SYS_finit_module,
                f.as_raw_fd(),
                c"".as_ptr(),
                0 as libc::c_int,
            )
        };
        if r != 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EEXIST) {
                return Err(format!("load the GPU driver ({m}): {e}"));
            }
        }
    }
    // The device nodes NVIDIA's libraries open (no udev here).
    let devices = fs::read_to_string("/proc/devices").unwrap_or_default();
    let major = |name: &str| {
        devices.lines().find_map(|l| {
            let (n, d) = l.trim().split_once(' ')?;
            (d.trim() == name).then(|| n.parse::<u32>().ok()).flatten()
        })
    };
    let gpus = fs::read_dir("/proc/driver/nvidia/gpus")
        .map(|d| d.count() as u32)
        .unwrap_or(0);
    if gpus == 0 {
        return Err("the GPU driver loaded but found no GPU".into());
    }
    let mut nodes = vec![("nvidiactl".to_string(), 195, 255)];
    for i in 0..gpus {
        nodes.push((format!("nvidia{i}"), 195, i));
    }
    if let Some(uvm) = major("nvidia-uvm") {
        nodes.push(("nvidia-uvm".into(), uvm, 0));
        nodes.push(("nvidia-uvm-tools".into(), uvm, 1));
    }
    for (name, maj, min) in nodes {
        let p = cstr(root.join("dev").join(&name));
        unsafe {
            libc::mknod(p.as_ptr(), libc::S_IFCHR | 0o666, libc::makedev(maj, min));
            libc::chmod(p.as_ptr(), 0o666);
        }
    }
    for (from, to) in [("/gpu/lib", NVIDIA_LIB), ("/gpu/bin", NVIDIA_BIN)] {
        if !Path::new(from).is_dir() {
            continue;
        }
        let target = root.join(to.trim_start_matches('/'));
        fs::create_dir_all(&target).ctx(to)?;
        mount(from, &target.to_string_lossy(), "", libc::MS_BIND, "")?;
    }
    Ok(())
}

/// The task process, between `fork` and `execve`. Never returns.
#[allow(clippy::too_many_arguments)]
fn child(
    stdin: &File,
    stdout: &File,
    stderr: &File,
    workdir: &str,
    uid: u32,
    gid: u32,
    path_var: &str,
    argv: Vec<CString>,
    envp: Vec<CString>,
) -> ! {
    unsafe {
        libc::setsid();
        libc::dup2(stdin.as_raw_fd(), 0);
        libc::dup2(stdout.as_raw_fd(), 1);
        libc::dup2(stderr.as_raw_fd(), 2);
    }
    let fail = |code: i32, msg: String| -> ! {
        eprintln!("pumasi: {msg}");
        exit_now(code)
    };
    let _ = fs::write("/proc/self/oom_score_adj", "0");
    let nofile = libc::rlimit {
        rlim_cur: 65536,
        rlim_max: 65536,
    };
    unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &nofile) };
    if let Err(e) = chroot(ROOT).and_then(|_| std::env::set_current_dir("/")) {
        fail(126, format!("enter root: {e}"));
    }
    if let Err(e) = std::env::set_current_dir(workdir) {
        fail(126, format!("working directory {workdir}: {e}"));
    }
    unsafe {
        if libc::setgroups(1, &gid) != 0 || libc::setgid(gid) != 0 || libc::setuid(uid) != 0 {
            fail(
                126,
                format!("switch to user {uid}:{gid}: {}", io::Error::last_os_error()),
            );
        }
    }
    let prog = argv[0].to_string_lossy().into_owned();
    let path = if prog.contains('/') {
        prog.clone()
    } else {
        let found = path_var
            .split(':')
            .filter(|d| !d.is_empty())
            .map(|d| format!("{d}/{prog}"))
            .find(|p| {
                let c = cstr(p);
                fs::metadata(p).is_ok_and(|m| m.is_file())
                    && unsafe { libc::access(c.as_ptr(), libc::X_OK) } == 0
            });
        match found {
            Some(p) => p,
            None => fail(127, format!("{prog}: command not found")),
        }
    };
    let mut argv_ptrs: Vec<*const libc::c_char> = argv.iter().map(|a| a.as_ptr()).collect();
    argv_ptrs.push(std::ptr::null());
    let mut envp_ptrs: Vec<*const libc::c_char> = envp.iter().map(|a| a.as_ptr()).collect();
    envp_ptrs.push(std::ptr::null());
    let c_path = cstr(&path);
    unsafe { libc::execve(c_path.as_ptr(), argv_ptrs.as_ptr(), envp_ptrs.as_ptr()) };
    let e = io::Error::last_os_error();
    let code = if e.raw_os_error() == Some(libc::ENOENT) {
        127
    } else {
        126
    };
    fail(code, format!("exec {path}: {e}"))
}

fn ensure_dir(p: &Path, mode: u32) -> R<()> {
    match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => return Ok(()),
        Ok(_) => remove_any(p).ctx(&p.display().to_string())?,
        Err(_) => {}
    }
    fs::create_dir_all(p).ctx(&p.display().to_string())?;
    fs::set_permissions(p, fs::Permissions::from_mode(mode)).ctx("chmod")?;
    Ok(())
}

/// Reads the input disk: the run spec, stdin, and the `/input` files.
fn read_input(input: impl Read, input_dir: &Path) -> R<RunSpec> {
    let mut ar = tar::Archive::new(BufReader::with_capacity(1 << 20, input));
    let mut spec: Option<RunSpec> = None;
    for entry in ar.entries().ctx("read input disk")? {
        let mut entry = entry.ctx("read input disk")?;
        let name = entry
            .path()
            .ctx("input path")?
            .to_string_lossy()
            .into_owned();
        if name == DISK_ROLE_ENTRY {
            continue;
        } else if name == SPEC_ENTRY {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).ctx("read spec")?;
            spec = Some(serde_json::from_slice(&buf).ctx("parse spec")?);
        } else if name == STDIN_ENTRY {
            let mut out = File::create(Path::new(TASK_DIR).join("stdin")).ctx("write stdin")?;
            io::copy(&mut entry, &mut out).ctx("write stdin")?;
        } else if let Some(rel) = name.strip_prefix(INPUT_PREFIX) {
            let Some(rel) = layer_path(Path::new(rel)) else {
                return Err(format!("bad input path {name}"));
            };
            let target = input_dir.join(&rel);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).ctx("create input dir")?;
            }
            let _ = remove_any(&target);
            let mut out = File::create(&target).ctx(&format!("write /input/{}", rel.display()))?;
            io::copy(&mut entry, &mut out).ctx("write input file")?;
            out.set_permissions(fs::Permissions::from_mode(0o644))
                .ctx("chmod")?;
        }
    }
    spec.ok_or_else(|| "input disk has no spec".into())
}

/// Mounts the kernel filesystems in the new root and sets up `/output`,
/// `/tmp`, `/etc/hosts` and loopback the way `docker run` would.
fn prepare_root(root: &Path, spec: &RunSpec, uid: u32, gid: u32) -> R<()> {
    for (dir, mode) in [
        ("proc", 0o555),
        ("sys", 0o555),
        ("dev", 0o755),
        ("tmp", 0o1777),
    ] {
        ensure_dir(&root.join(dir), mode)?;
    }
    let output = root.join("output");
    ensure_dir(&output, 0o755)?;
    let o = cstr(&output);
    unsafe { libc::chown(o.as_ptr(), uid, gid) };
    let r = |p: &str| root.join(p).to_string_lossy().into_owned();
    mount("proc", &r("proc"), "proc", 0, "")?;
    mount("sysfs", &r("sys"), "sysfs", libc::MS_RDONLY, "")?;
    mount("devtmpfs", &r("dev"), "devtmpfs", 0, "")?;
    let _ = fs::create_dir_all(root.join("dev/pts"));
    let _ = mount(
        "devpts",
        &r("dev/pts"),
        "devpts",
        0,
        "newinstance,ptmxmode=0666,mode=0620",
    );
    let _ = fs::create_dir_all(root.join("dev/shm"));
    let _ = mount("shm", &r("dev/shm"), "tmpfs", 0, "mode=1777");
    for (link, target) in [
        ("dev/fd", "/proc/self/fd"),
        ("dev/stdin", "/proc/self/fd/0"),
        ("dev/stdout", "/proc/self/fd/1"),
        ("dev/stderr", "/proc/self/fd/2"),
    ] {
        let _ = std::os::unix::fs::symlink(target, root.join(link));
    }
    let workdir = root.join(spec.workdir.trim_start_matches('/'));
    if !workdir.is_dir() {
        let _ = fs::create_dir_all(&workdir);
    }
    let hostname = if spec.hostname.is_empty() {
        "pumasi"
    } else {
        spec.hostname.as_str()
    };
    unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) };
    if root.join("etc").is_dir() {
        let _ = fs::write(
            root.join("etc/hosts"),
            format!("127.0.0.1\tlocalhost {hostname}\n::1\tlocalhost ip6-localhost ip6-loopback\n"),
        );
        let _ = fs::write(root.join("etc/hostname"), format!("{hostname}\n"));
    }
    loopback_up();
    if let Some(net) = &spec.network {
        network_up(net)?;
        if root.join("etc").is_dir() {
            let resolv = root.join("etc/resolv.conf");
            // Images often ship it as a symlink into /run; replace it.
            let _ = fs::remove_file(&resolv);
            let [a, b, c, d] = net.dns;
            let _ = fs::write(&resolv, format!("nameserver {a}.{b}.{c}.{d}\n"));
        }
    }
    Ok(())
}

fn sockaddr_in(ip: [u8; 4]) -> libc::sockaddr {
    let sin = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(ip),
        },
        sin_zero: [0; 8],
    };
    unsafe { std::mem::transmute::<libc::sockaddr_in, libc::sockaddr>(sin) }
}

/// Configures `eth0` and the default route, the way `ip addr add` and
/// `ip route add default` would.
fn network_up(net: &pumasi_guest::GuestNet) -> R<()> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return Err(format!("socket: {}", io::Error::last_os_error()));
        }
        let ifreq = || {
            let mut ifr: libc::ifreq = std::mem::zeroed();
            for (i, b) in b"eth0".iter().enumerate() {
                ifr.ifr_name[i] = *b as libc::c_char;
            }
            ifr
        };
        let check = |r: libc::c_int, what: &str| -> R<()> {
            if r < 0 {
                Err(format!("{what}: {}", io::Error::last_os_error()))
            } else {
                Ok(())
            }
        };
        let res = (|| -> R<()> {
            let mut ifr = ifreq();
            ifr.ifr_ifru.ifru_addr = sockaddr_in(net.address);
            check(
                libc::ioctl(fd, libc::SIOCSIFADDR as _, &ifr),
                "set the address of eth0 (is there a network card?)",
            )?;
            let mask = if net.prefix == 0 {
                0
            } else {
                u32::MAX << (32 - net.prefix.min(32) as u32)
            };
            let mut ifr = ifreq();
            ifr.ifr_ifru.ifru_netmask = sockaddr_in(mask.to_be_bytes());
            check(
                libc::ioctl(fd, libc::SIOCSIFNETMASK as _, &ifr),
                "set the netmask of eth0",
            )?;
            let mut ifr = ifreq();
            check(
                libc::ioctl(fd, libc::SIOCGIFFLAGS as _, &mut ifr),
                "read eth0 flags",
            )?;
            ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
            check(
                libc::ioctl(fd, libc::SIOCSIFFLAGS as _, &ifr),
                "bring eth0 up",
            )?;
            let mut rt: libc::rtentry = std::mem::zeroed();
            rt.rt_dst = sockaddr_in([0; 4]);
            rt.rt_genmask = sockaddr_in([0; 4]);
            rt.rt_gateway = sockaddr_in(net.gateway);
            rt.rt_flags = libc::RTF_UP | libc::RTF_GATEWAY;
            check(
                libc::ioctl(fd, libc::SIOCADDRT as _, &rt),
                "add the default route",
            )?;
            Ok(())
        })();
        libc::close(fd);
        res
    }
}

fn loopback_up() {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return;
        }
        let mut ifr: libc::ifreq = std::mem::zeroed();
        for (i, b) in b"lo".iter().enumerate() {
            ifr.ifr_name[i] = *b as libc::c_char;
        }
        if libc::ioctl(fd, libc::SIOCGIFFLAGS as _, &mut ifr) == 0 {
            ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
            libc::ioctl(fd, libc::SIOCSIFFLAGS as _, &ifr);
        }
        libc::close(fd);
    }
}

/// Resolves the image's `User` against its `/etc/passwd` and `/etc/group`.
fn resolve_user(root: &Path, user: &str) -> R<(u32, u32, String)> {
    let passwd = fs::read_to_string(root.join("etc/passwd")).unwrap_or_default();
    let group = fs::read_to_string(root.join("etc/group")).unwrap_or_default();
    let users: Vec<Vec<&str>> = passwd
        .lines()
        .map(|l| l.split(':').collect::<Vec<_>>())
        .filter(|f| f.len() >= 7)
        .collect();
    let (u, g) = match user.trim().split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user.trim(), None),
    };
    let entry = |pred: &dyn Fn(&Vec<&str>) -> bool| users.iter().find(|f| pred(f)).cloned();
    let (uid, pw) = if u.is_empty() {
        (0, entry(&|f| f[2] == "0"))
    } else if let Ok(n) = u.parse::<u32>() {
        (n, entry(&|f| f[2].parse() == Ok(n)))
    } else {
        let pw =
            entry(&|f| f[0] == u).ok_or_else(|| format!("user {u:?} not found in the image"))?;
        (pw[2].parse().ctx("passwd uid")?, Some(pw))
    };
    let gid = match g {
        Some(g) if !g.is_empty() => match g.parse::<u32>() {
            Ok(n) => n,
            Err(_) => group
                .lines()
                .map(|l| l.split(':').collect::<Vec<_>>())
                .find(|f| f.len() >= 3 && f[0] == g)
                .ok_or_else(|| format!("group {g:?} not found in the image"))?[2]
                .parse()
                .ctx("group gid")?,
        },
        _ => pw.as_ref().and_then(|f| f[3].parse().ok()).unwrap_or(0),
    };
    let home = pw
        .as_ref()
        .map(|f| f[5].to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| if uid == 0 { "/root".into() } else { "/".into() });
    Ok((uid, gid, home))
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

/// Seals a confidential task's result.
struct OutputSeal {
    key: [u8; 32],
    nonce: [u8; 32],
}

fn write_output(dev: &str, p: Payload, seal: Option<&OutputSeal>) -> io::Result<()> {
    let f = OpenOptions::new().write(true).open(dev)?;
    let f = match seal {
        None => write_tar(f, p)?,
        Some(s) => {
            let w = SealWriter::new(f, &s.key, Kind::Output, &s.nonce)?;
            write_tar(w, p)?.finish()?.0
        }
    };
    f.sync_all()
}

fn write_tar<W: Write>(out: W, mut p: Payload) -> io::Result<W> {
    let mut b = tar::Builder::new(BufWriter::with_capacity(1 << 20, out));
    b.mode(tar::HeaderMode::Deterministic);
    if let Some(path) = &p.stdout {
        p.result.stdout_dropped = append_capped(&mut b, STDOUT_ENTRY, path)?;
    }
    if let Some(path) = &p.stderr {
        p.result.stderr_dropped = append_capped(&mut b, STDERR_ENTRY, path)?;
    }
    if let Some(dir) = &p.output_dir {
        let mut files = Vec::new();
        walk(dir, dir, &mut files);
        files.sort();
        let mut total = 0u64;
        let mut kept = 0usize;
        for (rel, path, size) in files {
            if kept >= p.limits.1 || total + size > p.limits.0 || rel.len() > 4096 {
                p.result.files_dropped += 1;
                continue;
            }
            let Ok(f) = File::open(&path) else {
                continue;
            };
            let mut h = tar::Header::new_gnu();
            h.set_size(size);
            h.set_mode(0o644);
            h.set_entry_type(tar::EntryType::Regular);
            b.append_data(&mut h, format!("{OUTPUT_PREFIX}{rel}"), f.take(size))?;
            total += size;
            kept += 1;
        }
    }
    let json = serde_json::to_vec(&p.result).map_err(io::Error::other)?;
    let mut h = tar::Header::new_gnu();
    h.set_size(json.len() as u64);
    h.set_mode(0o644);
    b.append_data(&mut h, RESULT_ENTRY, json.as_slice())?;
    let w = b.into_inner()?;
    w.into_inner().map_err(|e| e.into_error())
}

/// Appends at most [`MAX_STREAM_BYTES`] of a file; returns the bytes left out.
fn append_capped<W: Write>(b: &mut tar::Builder<W>, name: &str, path: &Path) -> io::Result<u64> {
    let (f, len) = match File::open(path) {
        Ok(f) => {
            let len = f.metadata()?.len();
            (f, len)
        }
        Err(_) => return Ok(0),
    };
    let keep = len.min(MAX_STREAM_BYTES);
    let mut h = tar::Header::new_gnu();
    h.set_size(keep);
    h.set_mode(0o644);
    b.append_data(&mut h, name, f.take(keep))?;
    Ok(len - keep)
}

/// Regular files under `dir` (symlinks are not followed), with their path
/// relative to `base` and size.
fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, PathBuf, u64)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        let path = e.path();
        if ft.is_dir() {
            walk(base, &path, out);
        } else if ft.is_file() {
            let Ok(rel) = path.strip_prefix(base) else {
                continue;
            };
            let rel: Vec<String> = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect();
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            out.push((rel.join("/"), path, size));
        }
    }
}
