//! Messages from agents (before and after they are admitted) and the
//! replies agents read from the control plane.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pumasi_proto::msg::{
    AgentHello, FetchReply, FetchRequest, FromAgent, HelloReply, ToAgent, UpgradeHello,
};

fuzz_target!(|data: &[u8]| {
    let _ = AgentHello::decode(data);
    let _ = postcard::from_bytes::<FromAgent>(data);
    let _ = postcard::from_bytes::<ToAgent>(data);
    let _ = postcard::from_bytes::<HelloReply>(data);
    let _ = postcard::from_bytes::<FetchRequest>(data);
    let _ = postcard::from_bytes::<FetchReply>(data);
    let _ = postcard::from_bytes::<UpgradeHello>(data);
});
