//! Pumasi node agent: joins a cluster and runs the tasks it is assigned
//! (WASM, Docker, or OCI images in microVMs).

pub mod agent;
pub mod docker;
pub mod executor;
pub mod microvm;
mod tempdir;
pub mod upgrade;

pub use agent::{Agent, AgentConfig, Membership, default_node_name};
pub use executor::{RunResult, WasmExecutor};
pub use microvm::{MicrovmConfig, MicrovmExecutor};
pub use upgrade::{AgentExit, UpgradeConfig};
