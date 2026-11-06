//! Exported audit logs are verified by tools that read them back, and SSO
//! settings come from admins' JSON and URLs.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pumasi_proto::audit::{AuditEntry, AuditHead, GENESIS, verify_chain};
use pumasi_proto::sso::{SsoUpdate, check_url};

fuzz_target!(|data: &[u8]| {
    if let Ok(entries) = serde_json::from_slice::<Vec<AuditEntry>>(data) {
        let first = entries.first().map_or(1, |e| e.seq);
        let _ = verify_chain(GENESIS, first, &entries);
    }
    if let Ok(head) = serde_json::from_slice::<AuditHead>(data) {
        let _ = head.verify();
    }
    if let Ok(update) = serde_json::from_slice::<SsoUpdate>(data)
        && let SsoUpdate::Oidc(Some(o)) = &update
    {
        let _ = o.validate();
    }
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = check_url(s, &["https"], &["http"]);
    }
});
