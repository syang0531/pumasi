//! Pumasi node agent: joins a cluster and runs WASM tasks it is assigned.

pub mod agent;
pub mod docker;
pub mod executor;
mod tempdir;

pub use agent::{Agent, AgentConfig, Membership, default_node_name};
pub use executor::{RunResult, WasmExecutor};
