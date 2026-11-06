//! Tickets are pasted by people and passed around in chat, so every kind
//! is parsed from untrusted text.
#![no_main]
use std::str::FromStr;

use libfuzzer_sys::fuzz_target;
use pumasi_proto::ticket::{AdminTicket, JoinTicket, ReplicaTicket, SignInTicket};

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(t) = AdminTicket::from_str(s) {
        let again = AdminTicket::from_str(&t.to_string()).expect("round trip");
        assert_eq!(again.to_string(), t.to_string());
    }
    let _ = JoinTicket::from_str(s);
    let _ = ReplicaTicket::from_str(s);
    let _ = SignInTicket::from_str(s);
    let _ = pumasi_proto::job::JobId::from_str(s);
    let _ = pumasi_proto::store::StoreRef::from_str(s);
    let _ = pumasi_proto::store::parse_hash(s);
});
