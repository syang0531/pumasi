//! Shared types and wire protocol for Pumasi.
//!
//! Everything that crosses a process boundary lives here: the job
//! specification users write, the messages agents and the control plane
//! exchange, the admin API, and the tickets used to join a cluster.

pub mod job;
pub mod msg;
pub mod net;
pub mod ticket;
pub mod wire;

/// ALPN for the agent <-> control plane protocol.
pub const AGENT_ALPN: &[u8] = b"pumasi/agent/0";
/// ALPN for the admin (CLI) <-> control plane protocol.
pub const ADMIN_ALPN: &[u8] = b"pumasi/admin/0";
/// Bumped on any incompatible change to [`msg`].
pub const PROTOCOL_VERSION: u32 = 2;

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
