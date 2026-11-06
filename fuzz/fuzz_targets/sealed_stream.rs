//! Confidential-tier streams: arbitrary bytes must be refused cleanly,
//! and any change to a sealed stream must be detected.
#![no_main]
use std::io::{Cursor, Read};

use libfuzzer_sys::fuzz_target;
use pumasi_guest::seal::{Kind, SealReader, SealWriter, sealed_kind};

const KEY: [u8; 32] = [7; 32];

fuzz_target!(|data: &[u8]| {
    let _ = sealed_kind(data);
    let mut out = Vec::new();
    if let Ok(mut r) = SealReader::new(data, &KEY, Kind::Output, b"extra") {
        let _ = r.read_to_end(&mut out);
    }

    // Seal the input, flip one byte, and check that reading fails.
    let [a, b, mask, plain @ ..] = data else {
        return;
    };
    let mut w = SealWriter::new(Cursor::new(Vec::new()), &KEY, Kind::Input, b"x").unwrap();
    std::io::Write::write_all(&mut w, plain).unwrap();
    let (sealed, _) = w.finish().unwrap();
    let mut sealed = sealed.into_inner();
    let read = |bytes: &[u8]| {
        let mut out = Vec::new();
        SealReader::new(bytes, &KEY, Kind::Input, b"x")
            .and_then(|mut r| r.read_to_end(&mut out))?;
        Ok::<_, std::io::Error>(out)
    };
    assert_eq!(read(&sealed).expect("intact stream"), plain);
    if *mask != 0 {
        let at = usize::from(u16::from_le_bytes([*a, *b])) % sealed.len();
        sealed[at] ^= mask;
        assert!(
            read(&sealed).is_err(),
            "a changed byte at {at} went unnoticed"
        );
    }
});
