//! Shared types and wire protocol for Pumasi.
//!
//! Everything that crosses a process boundary lives here: the job
//! specification users write, the messages agents and the control plane
//! exchange, the admin API, and the tickets used to join a cluster.

pub mod audit;
pub mod blobs;
pub mod confidential;
pub mod economy;
pub mod gpu;
pub mod ha;
pub mod job;
pub mod manage;
pub mod monitor;
pub mod msg;
pub mod net;
pub mod perf;
pub mod power;
pub mod receipt;
pub mod registry;
pub mod release;
pub mod replica;
pub mod s3;
pub mod sso;
pub mod store;
pub mod ticket;
pub mod ui;
pub mod vmdisk;
pub mod webgpu;
pub mod wire;

/// ALPN for the agent <-> control plane protocol.
pub const AGENT_ALPN: &[u8] = b"pumasi/agent/0";
/// ALPN for the admin (CLI) <-> control plane protocol.
pub const ADMIN_ALPN: &[u8] = b"pumasi/admin/0";
/// ALPN of the file store: iroh-blobs' own protocol, served by the control
/// plane (and by agents, for blobs they cache).
pub const BLOBS_ALPN: &[u8] = iroh_blobs::ALPN;
/// ALPN between control plane replicas: Raft and replicas joining
/// (phase 13).
pub const RAFT_ALPN: &[u8] = b"pumasi/raft/0";
/// ALPN between agents for network probes (1.6): round trips and a short
/// transfer, see [`monitor::PROBE_BYTES`].
pub const PROBE_ALPN: &[u8] = b"pumasi/probe/0";
/// The first agent protocol with the file store.
pub const STORE_PROTOCOL: u32 = 8;
/// The first agent protocol with devices: GPUs, GPU challenges and microVM
/// networking.
pub const DEVICES_PROTOCOL: u32 = 9;
/// The first agent protocol with monitoring: node metrics and live task
/// output.
pub const MONITOR_PROTOCOL: u32 = 10;
/// The first agent protocol that follows control plane replicas: it is
/// told where the leader is ([`msg::HelloReply::NotLeader`]) and which
/// replicas there are ([`msg::ToAgent::Replicas`]).
pub const HA_PROTOCOL: u32 = 11;
/// The first agent protocol with WebGPU: it reports its adapters
/// ([`msg::FromAgent::WebGpu`]) and runs wasm tasks that ask for a GPU.
pub const WEBGPU_PROTOCOL: u32 = 12;
/// The first agent protocol for remote API clients (phase 15): it reports
/// its executors as Docker comes and goes and whether Docker GPU tasks can
/// draw ([`msg::FromAgent::Executors`]), the image digest each task ran
/// ([`msg::FromAgent::Image`]), and runs GPU tasks with driver
/// capabilities.
pub const REMOTE_PROTOCOL: u32 = 13;
/// The first agent protocol with confidential VMs (phase 16): it reports
/// the TEEs it can start ([`msg::FromAgent::Tee`]), runs confidential
/// tasks ([`msg::TaskAssignment::confidential`]) and passes their
/// attestation to the control plane ([`msg::FromAgent::Attest`],
/// [`msg::ToAgent::Keys`]).
pub const CONFIDENTIAL_PROTOCOL: u32 = 14;
/// What a protocol 14 peer knows beyond protocol 14, counted up by the
/// releases that add messages without changing the old ones. An agent
/// says its minor after its hello and the control plane after its welcome
/// ([`msg::HelloTail`], [`msg::WelcomeTail`], both ignored by a peer that
/// does not know them), and each side sends a newer message only to a
/// peer whose minor has it. The protocol version itself stays, so a
/// cluster upgrades in any order and without `--force`.
pub const PROTOCOL_MINOR: u32 = 4;
/// The first minor with sleep and wake (1.5): the agent says when its
/// machine is about to sleep ([`msg::FromAgent::Sleeping`]) and how it
/// sleeps and wakes ([`msg::FromAgent::Power`]), and sends magic packets
/// when asked ([`msg::ToAgent::Wake`]).
pub const POWER_MINOR: u32 = 1;
/// The first minor with the 1.6 monitoring: a microVM's output streams
/// while it runs (the agent's guest prints it on the VM console), and the
/// agent measures the network to other nodes when asked
/// ([`msg::ToAgent::Probe`], [`msg::FromAgent::Probe`]) and answers other
/// agents' probes on [`PROBE_ALPN`].
pub const MONITOR_MINOR: u32 = 2;
/// The first minor with settings from the dashboard (1.7): the agent
/// reports its availability settings ([`msg::FromAgent::Settings`]) and
/// changes them when asked ([`msg::ToAgent::Settings`]).
pub const SETTINGS_MINOR: u32 = 3;
/// The first minor with replicas (1.8): the agent keeps the dataset
/// versions it is told to ([`msg::ToAgent::Hold`]), checks them when asked
/// ([`msg::ToAgent::Scrub`]) and says what it has ([`msg::FromAgent::Held`]).
pub const REPLICA_MINOR: u32 = 4;
/// The protocol version as a literal, so it can be baked into
/// [`BUILD_INFO`] with `concat!`.
macro_rules! protocol_version {
    () => {
        14
    };
}

/// Bumped on any incompatible change to [`msg`].
pub const PROTOCOL_VERSION: u32 = protocol_version!();

/// The oldest agent protocol the control plane still accepts, so agents of
/// older releases can connect and take a rollout. Protocol 5 agents lack
/// the availability report (6), protocol 6 agents the performance profile
/// and benchmark challenges (7): they are billed at the reference score.
/// Protocol 7 agents lack the file store (8): they get no jobs that mount
/// datasets, write outputs to the store or use private images. Protocol 8
/// agents lack devices (9): they get no GPU jobs and no microVM jobs with
/// a network. Protocol 9 agents send no metrics and no live output.
/// Protocol 10 agents know only the control plane they joined: a replica
/// that is not the leader closes their connection, and they keep retrying
/// it. Protocol 11 agents have no WebGPU: they get no wasm jobs with GPUs.
/// Protocol 12 agents report their executors only when they connect and
/// no image digests: they get no GPU jobs with driver capabilities.
/// Protocol 13 agents start no confidential VMs: they get no confidential
/// jobs.
pub const MIN_AGENT_PROTOCOL: u32 = 5;

/// Marker baked into every binary: its version and protocol. `pumasi
/// upgrade rollout` reads it from a binary file without running it (the
/// binary may be for another OS), see [`release::read_build_info`].
pub const BUILD_INFO: &str = concat!(
    "pumasi-build-info:v=",
    env!("PUMASI_VERSION_STRING"),
    ";p=",
    protocol_version!(),
    ";"
);

/// This build's version: the release tag for release builds, else the
/// crate version (plus `+<build id>` when one was set).
pub fn version() -> &'static str {
    env!("PUMASI_VERSION_STRING")
}

/// Keeps [`BUILD_INFO`] in the binary. Called once at startup.
pub fn build_info() -> &'static str {
    // Opaque to the optimizer, so the marker cannot be dropped.
    std::hint::black_box(BUILD_INFO)
}

/// A 32-byte random secret (join tokens, admin credentials).
pub type Secret = [u8; 32];

/// Generates a fresh random secret.
pub fn new_secret() -> Secret {
    rand::random()
}

/// Compares two secrets in constant time.
pub fn secrets_eq(a: &Secret, b: &Secret) -> bool {
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Compares two strings in constant time (for their length).
pub fn secrets_eq_str(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// Content hash of a WASM module (BLAKE3).
pub type ModuleHash = [u8; 32];

/// Hashes module bytes.
pub fn module_hash(bytes: &[u8]) -> ModuleHash {
    *blake3::hash(bytes).as_bytes()
}

/// Short hex form of a hash for display.
pub fn short_hex(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&bytes[..bytes.len().min(6)])
}

/// Windows' `canonicalize` gives `\\?\C:\...`, which no mount point
/// (`C:\`) prefixes; drop the `\\?\` of a drive path.
pub fn without_verbatim(p: &std::path::Path) -> std::path::PathBuf {
    let s = p.to_string_lossy();
    match s.strip_prefix(r"\\?\") {
        Some(rest) if !rest.starts_with("UNC\\") => std::path::PathBuf::from(rest),
        _ => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn verbatim_drive_paths() {
        assert_eq!(
            without_verbatim(Path::new(r"\\?\C:\Users\a")),
            PathBuf::from(r"C:\Users\a")
        );
        let unc = Path::new(r"\\?\UNC\server\share");
        assert_eq!(without_verbatim(unc), unc);
        assert_eq!(without_verbatim(Path::new("/tmp")), PathBuf::from("/tmp"));
    }
}
