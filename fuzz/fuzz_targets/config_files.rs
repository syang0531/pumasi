//! Job and cluster files, their small value parsers, and the registry
//! references and credentials a job names.
#![no_main]
use std::str::FromStr;

use libfuzzer_sys::fuzz_target;
use pumasi_proto::{job, manage, registry, sso, store};

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let _ = job::JobFile::parse(s);
    let _ = manage::ClusterFile::parse(s);
    let _ = job::parse_duration_ms(s);
    if job::validate_rel_path(s).is_ok() {
        assert!(!s.starts_with('/') && !s.split(['/', '\\']).any(|p| p == ".."));
    }
    let _ = manage::Selector::from_str(s);
    let _ = manage::Cron::from_str(s);
    let _ = manage::Taint::from_str(s);
    let _ = store::parse_size(s);
    let _ = pumasi_proto::economy::parse_cu(s);
    let _ = registry::ImageRef::parse(s);
    let _ = registry::Credentials::parse(s);
    let _ = sso::Subject::from_str(s);
});
