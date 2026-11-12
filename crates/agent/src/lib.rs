//! Pumasi node agent: joins a cluster and runs the tasks it is assigned
//! (WASM, Docker, or OCI images in microVMs).

pub mod agent;
pub mod availability;
pub mod bench;
pub mod docker;
pub mod executor;
pub mod gpu;
pub mod metrics;
pub mod microvm;
pub mod platform;
pub mod power;
pub mod store;
pub mod tempdir;
pub mod upgrade;
pub mod webgpu;

pub use agent::{Agent, AgentConfig, Membership, default_node_name};
pub use availability::AvailabilityConfig;
pub use bench::BenchConfig;
pub use executor::{RunResult, WasmExecutor};
pub use microvm::{MicrovmConfig, MicrovmExecutor};
pub use power::PowerOptions;
pub use upgrade::{AgentExit, UpgradeConfig};
