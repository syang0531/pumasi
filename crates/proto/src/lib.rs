//! Shared types and wire protocol for Pumasi.
//!
//! Everything that crosses a process boundary lives here: the job
//! specification users write, the messages agents and the control plane
//! exchange, the admin API, and the tickets used to join a cluster.

pub mod economy;
pub mod job;
pub mod msg;
pub mod net;
pub mod receipt;
pub mod release;
pub mod ticket;
pub mod wire;

/// ALPN for the agent <-> control plane protocol.
pub const AGENT_ALPN: &[u8] = b"pumasi/agent/0";
/// ALPN for the admin (CLI) <-> control plane protocol.
pub const ADMIN_ALPN: &[u8] = b"pumasi/admin/0";
/// The protocol version as a literal, so it can be baked into
/// [`BUILD_INFO`] with `concat!`.
macro_rules! protocol_version {
    () => {
        5
    };
}

/// Bumped on any incompatible change to [`msg`].
pub const PROTOCOL_VERSION: u32 = protocol_version!();

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
