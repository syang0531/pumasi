//! Benchmarks and GPU challenges on NVIDIA MIG slices (1.16).
//!
//! CUDA shows a process at most one MIG slice, the one named in its
//! `CUDA_VISIBLE_DEVICES`, and only if that is set before CUDA starts. The
//! agent has CUDA open already (for the whole GPUs), so each slice is
//! measured, and answers its challenges, in a process of its own: this
//! binary again as `pumasi __cuda-slice measure|solve`, with the slice's
//! UUID in its environment, running the same kernels as a whole GPU
//! ([`super::cuda`]). Slices are then billed and checked like whole GPUs
//! instead of from an estimate scaled from their parent.

use std::{
    io::{Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use pumasi_proto::gpu::GpuChallenge;

use super::cuda::{Gpu, Measured};

/// The first argument that makes the `pumasi` binary measure a slice.
pub const SLICE_ARG: &str = "__cuda-slice";
/// The longest a slice's benchmark or challenge may take.
const SLICE_TIMEOUT: Duration = Duration::from_secs(180);

/// Where the slice processes come from: this binary, unless a library
/// user has none with [`serve`] in it.
fn exe() -> Result<PathBuf> {
    std::env::current_exe().context("find this binary")
}

fn command(uuid: &str, mode: &str) -> Result<Command> {
    let mut cmd = Command::new(exe()?);
    cmd.args([SLICE_ARG, mode])
        .env("CUDA_VISIBLE_DEVICES", uuid)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    Ok(cmd)
}

/// Runs a slice process with `input` on its stdin and returns its last
/// stdout line.
fn run(uuid: &str, mode: &str, input: &str) -> Result<String> {
    let mut child = command(uuid, mode)?
        .spawn()
        .with_context(|| format!("start the {mode} process for {uuid}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input.as_bytes())?;
    }
    let mut stdout = child.stdout.take().context("stdout")?;
    let mut stderr = child.stderr.take().context("stderr")?;
    let reader = std::thread::spawn(move || {
        let mut out = String::new();
        let _ = stdout.read_to_string(&mut out);
        out
    });
    let err_reader = std::thread::spawn(move || {
        let mut err = String::new();
        let _ = stderr.read_to_string(&mut err);
        err
    });
    let until = std::time::Instant::now() + SLICE_TIMEOUT;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if std::time::Instant::now() > until {
            let _ = child.kill();
            let _ = child.wait();
            bail!("the {mode} of {uuid} took longer than {SLICE_TIMEOUT:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let out = reader.join().unwrap_or_default();
    let err = err_reader.join().unwrap_or_default();
    if !status.success() {
        bail!(
            "{}",
            err.trim()
                .lines()
                .last()
                .unwrap_or("the slice process failed")
        );
    }
    out.lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .map(str::to_string)
        .context("the slice process said nothing")
}

/// Measures FP32, memory bandwidth and the hash rate of slice `uuid`.
pub fn measure(uuid: &str, vram_mib: u64) -> Result<Measured> {
    parse_measured(&run(uuid, "measure", &format!("{vram_mib}\n"))?)
}

/// Answers a GPU challenge on slice `uuid`. Empty when it cannot.
pub fn solve(uuid: &str, c: &GpuChallenge) -> Vec<u64> {
    let input = serde_json::to_string(c).unwrap_or_default();
    match run(uuid, "solve", &input).and_then(|l| Ok(serde_json::from_str(&l)?)) {
        Ok(xs) => xs,
        Err(e) => {
            tracing::warn!(slice = %uuid, "GPU check failed: {e:#}");
            Vec::new()
        }
    }
}

fn parse_measured(line: &str) -> Result<Measured> {
    let v: Vec<u64> = line
        .strip_prefix("measured ")
        .context("not a measurement")?
        .split_whitespace()
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()
        .context("not a measurement")?;
    let [gflops, bw, hash] = v[..] else {
        bail!("not a measurement");
    };
    Ok(Measured {
        fp32_gflops: u32::try_from(gflops)?,
        mem_bw_gb_s: u32::try_from(bw)?,
        hash_rate: hash,
    })
}

/// `pumasi __cuda-slice measure|solve`: runs on the one slice CUDA shows
/// this process. Returns the exit code.
pub fn serve(mode: &str) -> i32 {
    let mut input = String::new();
    let _ = std::io::stdin().read_to_string(&mut input);
    let r = (|| -> Result<String> {
        let gpu = Gpu::open_visible()?;
        match mode {
            "measure" => {
                let vram: u64 = input.trim().parse().context("the slice's memory")?;
                let m = gpu.measure(vram)?;
                Ok(format!(
                    "measured {} {} {}",
                    m.fp32_gflops, m.mem_bw_gb_s, m.hash_rate
                ))
            }
            "solve" => {
                let c: GpuChallenge = serde_json::from_str(&input).context("the challenge")?;
                Ok(serde_json::to_string(&gpu.solve(&c)?)?)
            }
            other => bail!("unknown mode {other:?}"),
        }
    })();
    match r {
        Ok(line) => {
            println!("{line}");
            0
        }
        Err(e) => {
            eprintln!("{e:#}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurements_round_trip() {
        let m = parse_measured("measured 9500 600 123456789").unwrap();
        assert_eq!(
            (m.fp32_gflops, m.mem_bw_gb_s, m.hash_rate),
            (9500, 600, 123_456_789)
        );
        assert!(parse_measured("measured 1 2").is_err());
        assert!(parse_measured("hello").is_err());
        let cmd = command("MIG-abc", "measure").unwrap();
        assert!(
            cmd.get_envs()
                .any(|(k, v)| k == "CUDA_VISIBLE_DEVICES" && v == Some("MIG-abc".as_ref()))
        );
        assert_eq!(cmd.get_args().collect::<Vec<_>>(), [SLICE_ARG, "measure"]);
    }
}
