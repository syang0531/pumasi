//! Windows gives a program's main thread 1 MiB of stack, which the CLI's
//! argument parser and command dispatch (large enums, unoptimized in debug
//! builds) can overflow. Ask for 16 MiB, like other platforms' defaults.

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("windows") {
        if target.contains("msvc") {
            println!("cargo:rustc-link-arg-bins=/STACK:16777216");
        } else {
            println!("cargo:rustc-link-arg-bins=-Wl,--stack,16777216");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
}
