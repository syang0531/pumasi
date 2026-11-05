//! Release builds embed the microVM guest init (a static Linux binary of
//! `pumasi-guest`) so `pumasi vm setup` needs nothing else: set
//! `PUMASI_EMBED_GUEST` to its absolute path. Other builds embed nothing and find
//! the guest in the VM directory or next to the binary.

use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=PUMASI_EMBED_GUEST");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR")).join("guest.rs");
    let code = match env::var_os("PUMASI_EMBED_GUEST").filter(|p| !p.is_empty()) {
        Some(path) => {
            let path = fs::canonicalize(&path).unwrap_or_else(|e| {
                panic!("PUMASI_EMBED_GUEST={path:?} (must be an absolute path): {e}")
            });
            println!("cargo:rerun-if-changed={}", path.display());
            let head = fs::read(&path).expect("read PUMASI_EMBED_GUEST");
            assert!(
                head.starts_with(b"\x7fELF"),
                "PUMASI_EMBED_GUEST is not a Linux binary"
            );
            format!("Some(include_bytes!({:?}))", path.display().to_string())
        }
        None => "None".to_string(),
    };
    fs::write(
        &out,
        format!("pub const EMBEDDED_GUEST: Option<&[u8]> = {code};\n"),
    )
    .expect("write guest.rs");
}
