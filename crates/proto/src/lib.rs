//! Shared types and wire protocol for Pumasi.
//!
//! Everything that crosses a process boundary lives here: the job
//! specification users write, the messages agents and the control plane
//! exchange, the admin API, and the tickets used to join a cluster.

pub mod blobs;
pub mod economy;
pub mod gpu;
pub mod job;
pub mod manage;
pub mod monitor;
pub mod msg;
pub mod net;
pub mod perf;
pub mod receipt;
pub mod registry;
pub mod release;
pub mod store;
pub mod ticket;
pub mod wire;

/// ALPN for the agent <-> control plane protocol.
pub const AGENT_ALPN: &[u8] = b"pumasi/agent/0";
/// ALPN for the admin (CLI) <-> control plane protocol.
pub const ADMIN_ALPN: &[u8] = b"pumasi/admin/0";
/// ALPN of the file store: iroh-blobs' own protocol, served by the control
/// plane (and by agents, for blobs they cache).
pub const BLOBS_ALPN: &[u8] = iroh_blobs::ALPN;
/// The first agent protocol with the file store.
pub const STORE_PROTOCOL: u32 = 8;
/// The first agent protocol with devices: GPUs, GPU challenges and microVM
/// networking.
pub const DEVICES_PROTOCOL: u32 = 9;
/// The first agent protocol with monitoring: node metrics and live task
/// output.
pub const MONITOR_PROTOCOL: u32 = 10;
/// The protocol version as a literal, so it can be baked into
/// [`BUILD_INFO`] with `concat!`.
macro_rules! protocol_version {
    () => {
        10
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
