//! The microVM disks both the agent and the control plane write and read:
//! the run spec of an image job, the input and layers tars, and the
//! guest's output tar (see `pumasi_guest` for the protocol). The agent
//! writes them as disks; for a confidential task the control plane writes
//! the same tars sealed (phase 16) and reads the guest's sealed output.

use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use pumasi_guest::{
    DISK_ROLE_ENTRY, GuestOutcome, GuestResult, INPUT_PREFIX, OUTPUT_PREFIX, RESULT_ENTRY,
    ROLE_INPUT, ROLE_LAYERS, RunSpec, SPEC_ENTRY, STDERR_ENTRY, STDIN_ENTRY, STDOUT_ENTRY,
};

use crate::{
    job::{
        DockerSpec, FileEntry, INPUT_DIR, MAX_OUTPUT_BYTES, MAX_OUTPUT_FILES_BYTES, TaskInput,
        validate_rel_path,
    },
    msg::Outcome,
    registry::ContainerConfig,
    store::MAX_STORE_OUTPUT_BYTES,
};

/// What the guest runs: the image's entrypoint, then the job's command and
/// task args (or the image's `Cmd` when both are empty), like `docker run`.
pub fn run_spec(config: &ContainerConfig, spec: &DockerSpec, input: &TaskInput) -> RunSpec {
    let override_cmd: Vec<String> = spec
        .command
        .iter()
        .chain(input.args.iter())
        .cloned()
        .collect();
    let cmd = if override_cmd.is_empty() {
        config.cmd.clone().unwrap_or_default()
    } else {
        override_cmd
    };
    let mut argv = config.entrypoint.clone().unwrap_or_default();
    argv.extend(cmd);
    let mut env: Vec<String> = config.env.clone().unwrap_or_default();
    let mut set = |k: &str, v: &str| {
        let prefix = format!("{k}=");
        env.retain(|e| !e.starts_with(&prefix));
        env.push(format!("{k}={v}"));
    };
    set("HOSTNAME", "pumasi");
    for (k, v) in &input.env {
        set(k, v);
    }
    RunSpec {
        argv,
        env,
        workdir: INPUT_DIR.into(),
        user: config.user.clone().unwrap_or_default(),
        hostname: "pumasi".into(),
        ..Default::default()
    }
}

fn tar_entry<W: Write>(b: &mut tar::Builder<W>, name: &str, data: &[u8]) -> Result<()> {
    let mut h = tar::Header::new_gnu();
    h.set_size(data.len() as u64);
    h.set_mode(0o644);
    h.set_entry_type(tar::EntryType::Regular);
    b.append_data(&mut h, name, data)?;
    Ok(())
}

/// Writes the layers tar: the role entry, then each layer blob in order.
pub fn write_layers_tar<W: Write>(out: W, blobs: &[PathBuf]) -> Result<W> {
    let mut b = tar::Builder::new(out);
    b.mode(tar::HeaderMode::Deterministic);
    tar_entry(&mut b, DISK_ROLE_ENTRY, ROLE_LAYERS.as_bytes())?;
    for (i, blob) in blobs.iter().enumerate() {
        let mut f =
            std::fs::File::open(blob).with_context(|| format!("read {}", blob.display()))?;
        b.append_file(format!("layer-{i:03}"), &mut f)?;
    }
    Ok(b.into_inner()?)
}

/// Writes the input tar: the role entry, the run spec, stdin, then the
/// files from the file store (`staged`: path in `/input`, file, size), the
/// job's shared files and the task's own; later ones win.
pub fn write_input_tar<W: Write>(
    out: W,
    spec: &RunSpec,
    stdin: &[u8],
    shared: &[FileEntry],
    own: &[FileEntry],
    staged: &[(String, PathBuf, u64)],
) -> Result<W> {
    let mut b = tar::Builder::new(out);
    b.mode(tar::HeaderMode::Deterministic);
    tar_entry(&mut b, DISK_ROLE_ENTRY, ROLE_INPUT.as_bytes())?;
    tar_entry(&mut b, SPEC_ENTRY, &serde_json::to_vec(spec)?)?;
    tar_entry(&mut b, STDIN_ENTRY, stdin)?;
    for (rel, file, size) in staged {
        validate_rel_path(rel)?;
        let mut h = tar::Header::new_gnu();
        h.set_size(*size);
        h.set_mode(0o644);
        h.set_entry_type(tar::EntryType::Regular);
        let f = std::fs::File::open(file).with_context(|| format!("read {}", file.display()))?;
        b.append_data(&mut h, format!("{INPUT_PREFIX}{rel}"), f.take(*size))?;
    }
    for f in shared.iter().chain(own) {
        validate_rel_path(&f.path)?;
        tar_entry(&mut b, &format!("{INPUT_PREFIX}{}", f.path), &f.data)?;
    }
    Ok(b.into_inner()?)
}

/// What the guest wrote to its output disk.
#[derive(Debug)]
pub struct GuestOutput {
    pub result: GuestResult,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub files: Vec<FileEntry>,
}

/// Parses the guest's output tar. The guest is untrusted: entries are only
/// read into memory, within the caps, or with `kept` written as plain
/// files under that directory (within the file store's caps).
pub fn parse_output(r: impl Read, kept: Option<&Path>) -> Result<GuestOutput> {
    let mut ar = tar::Archive::new(r);
    let mut result = None;
    let (mut stdout, mut stderr, mut files) = (Vec::new(), Vec::new(), Vec::new());
    let mut total = 0u64;
    let read = |e: &mut dyn Read, cap: u64| -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        e.take(cap).read_to_end(&mut buf)?;
        Ok(buf)
    };
    for entry in ar.entries().context("read the output")? {
        let mut entry = entry.context("read the output")?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let name = entry.path()?.to_string_lossy().into_owned();
        if name == RESULT_ENTRY {
            result = Some(
                serde_json::from_slice::<GuestResult>(&read(&mut entry, 64 * 1024)?)
                    .context("bad result from the guest")?,
            );
        } else if name == STDOUT_ENTRY {
            stdout = read(&mut entry, MAX_OUTPUT_BYTES as u64)?;
        } else if name == STDERR_ENTRY {
            stderr = read(&mut entry, MAX_OUTPUT_BYTES as u64)?;
        } else if let Some(rel) = name.strip_prefix(OUTPUT_PREFIX) {
            if validate_rel_path(rel).is_err() {
                continue;
            }
            let size = entry.header().size()?;
            if let Some(dir) = kept {
                if total + size > MAX_STORE_OUTPUT_BYTES {
                    continue;
                }
                let target = dir.join(rel);
                if let Some(p) = target.parent() {
                    std::fs::create_dir_all(p)?;
                }
                let mut f = std::fs::File::create(&target)?;
                total += std::io::copy(&mut Read::take(&mut entry, size), &mut f)?;
                continue;
            }
            if total + size > MAX_OUTPUT_FILES_BYTES as u64 {
                continue;
            }
            let data = read(&mut entry, size)?;
            total += data.len() as u64;
            files.push(FileEntry {
                path: rel.to_string(),
                data,
            });
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let result = result.context("the guest's result is incomplete")?;
    Ok(GuestOutput {
        result,
        stdout,
        stderr,
        files,
    })
}

/// Notes in stderr what the guest left out because of the caps.
pub fn note_dropped(stderr: &mut Vec<u8>, r: &GuestResult) {
    if r.stdout_dropped > 0 || r.stderr_dropped > 0 {
        stderr.extend_from_slice(
            format!(
                "\n[pumasi] output over {} MiB was dropped ({} bytes of stdout, {} of stderr)\n",
                MAX_OUTPUT_BYTES >> 20,
                r.stdout_dropped,
                r.stderr_dropped
            )
            .as_bytes(),
        );
    }
    if r.files_dropped > 0 {
        stderr.extend_from_slice(
            format!(
                "\n[pumasi] output files exceeded {} MiB; {} were dropped\n",
                MAX_OUTPUT_FILES_BYTES >> 20,
                r.files_dropped
            )
            .as_bytes(),
        );
    }
}

/// How a guest outcome is reported, the way Docker reports it: exit code
/// as is, death by signal N as `128 + N`, the OOM killer as a trap.
pub fn outcome_of(o: &GuestOutcome, memory_bytes: u64) -> Outcome {
    match o {
        GuestOutcome::Exited(code) => Outcome::Exited(*code),
        GuestOutcome::Signaled(sig) => Outcome::Exited(128 + sig),
        GuestOutcome::OutOfMemory => {
            Outcome::Trap(format!("out of memory (limit {} MiB)", memory_bytes >> 20))
        }
        GuestOutcome::Failed(msg) => Outcome::Error(format!("microVM: {msg}")),
    }
}
