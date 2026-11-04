//! Bakes the version string into the binary. Release builds set
//! `PUMASI_VERSION` (from the git tag); other builds use the crate version,
//! with `+<PUMASI_BUILD_ID>` appended when that is set, so two builds of the
//! same version can be told apart.

fn main() {
    println!("cargo:rerun-if-env-changed=PUMASI_VERSION");
    println!("cargo:rerun-if-env-changed=PUMASI_BUILD_ID");
    let version = match std::env::var("PUMASI_VERSION") {
        Ok(v) if !v.trim().is_empty() => v.trim().trim_start_matches('v').to_string(),
        _ => {
            let base = std::env::var("CARGO_PKG_VERSION").unwrap();
            match std::env::var("PUMASI_BUILD_ID") {
                Ok(id) if !id.trim().is_empty() => format!("{base}+{}", id.trim()),
                _ => base,
            }
        }
    };
    assert!(
        !version.is_empty()
            && version.len() <= 64
            && version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "+-._".contains(c)),
        "PUMASI_VERSION / PUMASI_BUILD_ID may use only letters, digits and + - . _ (got {version:?})"
    );
    println!("cargo:rustc-env=PUMASI_VERSION_STRING={version}");
}
